#!/usr/bin/env python
"""
Database diagnostics — run against any offsets-db PostgreSQL instance.

Usage
-----
    # Local (reads OFFSETS_DB_DATABASE_URL from .env)
    pixi run python scripts/db_diagnostics.py

    # Override the URL
    pixi run python scripts/db_diagnostics.py --url postgresql://user:pass@host/db

    # Only specific sections
    pixi run python scripts/db_diagnostics.py --section indexes --section vacuum
"""

from __future__ import annotations

import os
import sys

import click
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(override=False)

# ── Sections ──────────────────────────────────────────────────────────────────
ALL_SECTIONS = ['overview', 'tables', 'indexes', 'usage', 'vacuum', 'cache', 'bloat']

# ── Formatting helpers ─────────────────────────────────────────────────────────

RESET = '\x1b[0m'
BOLD = '\x1b[1m'
DIM = '\x1b[2m'
GREEN = '\x1b[32m'
YELLOW = '\x1b[33m'
RED = '\x1b[31m'
CYAN = '\x1b[36m'
BLUE = '\x1b[34m'


def _c(text: str, *codes: str) -> str:
    return ''.join(codes) + str(text) + RESET


def _header(title: str) -> None:
    width = 72
    click.echo()
    click.echo(_c('─' * width, DIM))
    click.echo(_c(f'  {title}', BOLD, CYAN))
    click.echo(_c('─' * width, DIM))


def _table(rows: list[dict], columns: list[tuple[str, str, int]]) -> None:
    """
    Print a fixed-width table.

    columns: list of (key, header, width) tuples
    """
    # header row
    header = '  '.join(f'{_c(hdr, BOLD):<{w + len(BOLD) + len(RESET)}}' for _, hdr, w in columns)
    click.echo(header)
    sep = '  '.join('─' * w for _, _, w in columns)
    click.echo(_c(sep, DIM))
    for row in rows:
        parts = []
        for key, _, w in columns:
            val = str(row.get(key, ''))
            parts.append(f'{val:<{w}}')
        click.echo('  '.join(parts))
    if not rows:
        click.echo(_c('  (no rows)', DIM))


def _kv(label: str, value: str, *, color: str = '') -> None:
    label_fmt = _c(f'{label:<28}', BOLD)
    value_fmt = _c(value, color) if color else value
    click.echo(f'  {label_fmt}  {value_fmt}')


# ── Query helpers ──────────────────────────────────────────────────────────────


def _q(cur, sql: str, params=None) -> list[dict]:
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def _scalar(cur, sql: str, params=None):
    cur.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    # RealDictCursor returns a dict; grab the first value regardless of key name.
    return next(iter(row.values()))


# ── Sections ──────────────────────────────────────────────────────────────────


def section_overview(cur) -> None:
    _header('Overview')

    pg_version = _scalar(cur, 'SELECT version()')
    db_name = _scalar(cur, 'SELECT current_database()')
    db_size = _scalar(cur, 'SELECT pg_size_pretty(pg_database_size(current_database()))')
    conn_count = _scalar(cur, 'SELECT count(*) FROM pg_stat_activity')
    max_conn = _scalar(cur, 'SELECT setting FROM pg_settings WHERE name = %s', ('max_connections',))
    stats_reset = _scalar(cur, 'SELECT stats_reset FROM pg_stat_bgwriter')

    _kv('Database', db_name)
    _kv('Size', db_size, color=CYAN)
    _kv('Connections', f'{conn_count} / {max_conn}')
    _kv('Stats reset at', str(stats_reset) if stats_reset else 'never')
    _kv('PostgreSQL', (pg_version or '').split(' on ')[0])


def section_tables(cur) -> None:
    _header('Table Sizes')

    rows = _q(
        cur,
        """
        SELECT
            t.relname                                        AS table_name,
            to_char(c.reltuples::bigint, '999,999,999,999') AS est_rows,
            pg_size_pretty(pg_table_size(t.relid))          AS table_size,
            pg_size_pretty(pg_indexes_size(t.relid))        AS index_size,
            pg_size_pretty(pg_total_relation_size(t.relid)) AS total_size
        FROM pg_stat_user_tables t
        JOIN pg_class c ON c.relname = t.relname
        ORDER BY pg_total_relation_size(t.relid) DESC
    """,
    )

    _table(
        rows,
        [
            ('table_name', 'Table', 20),
            ('est_rows', 'Est. Rows', 18),
            ('table_size', 'Table', 10),
            ('index_size', 'Indexes', 10),
            ('total_size', 'Total', 10),
        ],
    )


def section_indexes(cur) -> None:
    _header('Index Inventory')

    rows = _q(
        cur,
        """
        SELECT
            i.relname                        AS index_name,
            t.relname                        AS table_name,
            am.amname                        AS type,
            pg_size_pretty(pg_relation_size(i.oid)) AS size,
            ix.indisunique                   AS unique,
            pg_get_indexdef(i.oid)           AS definition
        FROM pg_index ix
        JOIN pg_class i  ON i.oid  = ix.indexrelid
        JOIN pg_class t  ON t.oid  = ix.indrelid
        JOIN pg_am    am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = i.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname NOT LIKE 'pg_%'
          AND t.relname NOT LIKE 'alembic_%'
        ORDER BY t.relname, i.relname
    """,
    )

    for row in rows:
        u = _c(' UNIQUE', GREEN) if row['unique'] else ''
        click.echo(
            f'  {_c(row["index_name"], BOLD):<60}'
            f'  {_c(row["table_name"], CYAN):<20}'
            f'  {_c(row["type"].upper(), YELLOW):<8}'
            f'  {row["size"]:>8}'
            f'{u}'
        )
        click.echo(_c(f'    {row["definition"]}', DIM))

    if not rows:
        click.echo(_c('  (no indexes found)', DIM))


def section_usage(cur) -> None:
    _header('Index Usage  (since last stats reset)')

    rows = _q(
        cur,
        """
        SELECT
            ui.relname                       AS table_name,
            ui.indexrelname                  AS index_name,
            ui.idx_scan                      AS scans,
            ui.idx_tup_read                  AS tuples_read,
            ui.idx_tup_fetch                 AS tuples_fetched,
            pg_size_pretty(pg_relation_size(ui.indexrelid)) AS size,
            CASE WHEN ui.idx_scan = 0 THEN 'YES' ELSE '' END AS unused
        FROM pg_stat_user_indexes ui
        JOIN pg_index ix ON ix.indexrelid = ui.indexrelid
        WHERE NOT ix.indisprimary
        ORDER BY ui.idx_scan ASC, ui.relname, ui.indexrelname
    """,
    )

    cols = [
        ('table_name', 'Table', 20),
        ('index_name', 'Index', 44),
        ('scans', 'Scans', 8),
        ('tuples_fetched', 'Rows Fetched', 12),
        ('size', 'Size', 8),
        ('unused', 'Unused?', 7),
    ]

    # colorise the 'unused' flag
    for row in rows:
        if row['unused']:
            row['unused'] = _c('YES', YELLOW)
        row['scans'] = f'{row["scans"]:,}'
        row['tuples_fetched'] = f'{row["tuples_fetched"]:,}'

    _table(rows, cols)

    unused = sum(1 for r in rows if 'YES' in str(r.get('unused', '')))
    if unused:
        click.echo()
        click.echo(_c(f'  ⚠  {unused} non-primary index(es) have never been scanned.', YELLOW))


def section_vacuum(cur) -> None:
    _header('Vacuum & Analyze Stats')

    rows = _q(
        cur,
        """
        SELECT
            relname                                          AS table_name,
            n_live_tup                                       AS live,
            n_dead_tup                                       AS dead,
            CASE WHEN n_live_tup > 0
                 THEN round(100.0 * n_dead_tup / n_live_tup, 1)
                 ELSE 0
            END                                              AS dead_pct,
            to_char(last_vacuum,     'YYYY-MM-DD HH24:MI')  AS last_vacuum,
            to_char(last_autovacuum, 'YYYY-MM-DD HH24:MI')  AS last_autovacuum,
            to_char(last_analyze,    'YYYY-MM-DD HH24:MI')  AS last_analyze,
            to_char(last_autoanalyze,'YYYY-MM-DD HH24:MI')  AS last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
        FROM pg_stat_user_tables
        ORDER BY n_dead_tup DESC
    """,
    )

    for row in rows:
        dead_pct = float(row['dead_pct'] or 0)
        pct_color = RED if dead_pct > 10 else YELLOW if dead_pct > 5 else GREEN
        click.echo()
        click.echo(f'  {_c(row["table_name"], BOLD, CYAN)}')
        _kv(
            'Live / Dead rows',
            f'{row["live"]:,} / {_c(str(row["dead"]), pct_color)} '
            f'({_c(str(row["dead_pct"]) + "%", pct_color)} dead)',
        )
        _kv('Last vacuum', row['last_vacuum'] or '—')
        _kv('Last autovacuum', row['last_autovacuum'] or '—')
        _kv('Last analyze', row['last_analyze'] or '—')
        _kv('Last autoanalyze', row['last_autoanalyze'] or '—')
        _kv('Vacuum counts', f'manual={row["vacuum_count"]}  auto={row["autovacuum_count"]}')
        _kv('Analyze counts', f'manual={row["analyze_count"]}  auto={row["autoanalyze_count"]}')


def section_cache(cur) -> None:
    _header('Cache Hit Rates')

    # Table (heap) cache hit
    table_rows = _q(
        cur,
        """
        SELECT
            relname                                            AS table_name,
            heap_blks_read + heap_blks_hit                    AS total_reads,
            CASE WHEN heap_blks_read + heap_blks_hit > 0
                 THEN round(100.0 * heap_blks_hit /
                      (heap_blks_read + heap_blks_hit), 2)
                 ELSE NULL
            END                                                AS heap_hit_pct,
            CASE WHEN idx_blks_read + idx_blks_hit > 0
                 THEN round(100.0 * idx_blks_hit /
                      (idx_blks_read + idx_blks_hit), 2)
                 ELSE NULL
            END                                                AS idx_hit_pct
        FROM pg_statio_user_tables
        ORDER BY total_reads DESC
    """,
    )

    click.echo()
    click.echo(_c('  Table cache hits (heap + index)', BOLD))
    click.echo()
    for row in table_rows:

        def _pct(val) -> str:
            if val is None:
                return _c('  n/a', DIM)
            v = float(val)
            color = GREEN if v >= 99 else YELLOW if v >= 95 else RED
            return _c(f'{v:6.2f}%', color)

        click.echo(
            f'  {_c(row["table_name"], CYAN):<22}'
            f'  heap={_pct(row["heap_hit_pct"])}'
            f'  idx={_pct(row["idx_hit_pct"])}'
        )

    # Overall DB cache hit
    click.echo()
    db_hit = _scalar(
        cur,
        """
        SELECT round(100.0 * sum(blks_hit) / nullif(sum(blks_hit) + sum(blks_read), 0), 2)
        FROM pg_stat_database
        WHERE datname = current_database()
    """,
    )
    color = (
        GREEN
        if db_hit and float(db_hit) >= 99
        else YELLOW
        if db_hit and float(db_hit) >= 95
        else RED
    )
    _kv('Overall DB cache hit', _c(f'{db_hit}%', color) if db_hit else '—', color=color)


def section_bloat(cur) -> None:
    _header('Approximate Table & Index Bloat')

    rows = _q(
        cur,
        """
        WITH constants AS (
            SELECT current_setting('block_size')::int AS bs
        ),
        table_stats AS (
            SELECT
                schemaname,
                tablename,
                (datawidth + nullhdr + 4) AS row_size,
                reltuples,
                relpages,
                bs
            FROM (
                SELECT
                    s.schemaname,
                    s.tablename,
                    c.reltuples,
                    c.relpages,
                    k.bs,
                    24 + sum(
                        CASE
                            WHEN a.atttypid IN (16,17,18,19,20,21,22,23,24,25,26)
                                 OR a.atttypid::regtype::text IN ('text','varchar','bpchar','bytea')
                            THEN 8
                            ELSE 4
                        END
                    ) AS datawidth,
                    23 AS nullhdr
                FROM pg_stats s
                JOIN pg_class c ON c.relname = s.tablename
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = s.attname
                CROSS JOIN constants k
                WHERE s.schemaname = 'public'
                GROUP BY s.schemaname, s.tablename, c.reltuples, c.relpages, k.bs
            ) x
        )
        SELECT
            tablename                                               AS table_name,
            relpages * bs                                          AS real_bytes,
            pg_size_pretty(relpages * bs::bigint)                  AS real_size,
            CASE WHEN relpages > 0
                 THEN round((100.0 *
                      (1 - (reltuples * row_size) / (relpages * bs::float)))::numeric, 1)
                 ELSE 0
            END                                                     AS bloat_pct
        FROM table_stats
        ORDER BY bloat_pct DESC
    """,
    )

    for row in rows:
        pct = float(row['bloat_pct'] or 0)
        color = RED if pct > 30 else YELLOW if pct > 10 else GREEN
        click.echo(
            f'  {_c(row["table_name"], CYAN):<22}'
            f'  size={row["real_size"]:>8}'
            f'  bloat≈{_c(f"{pct:.1f}%", color)}'
        )

    if not rows:
        click.echo(_c('  (no data — tables may be empty)', DIM))


# ── CLI ────────────────────────────────────────────────────────────────────────

SECTION_FNS = {
    'overview': section_overview,
    'tables': section_tables,
    'indexes': section_indexes,
    'usage': section_usage,
    'vacuum': section_vacuum,
    'cache': section_cache,
    'bloat': section_bloat,
}


@click.command()
@click.option(
    '--url',
    '-u',
    default=lambda: os.environ.get('OFFSETS_DB_DATABASE_URL', ''),
    show_default='$OFFSETS_DB_DATABASE_URL',
    help='PostgreSQL connection URL.',
)
@click.option(
    '--section',
    '-s',
    multiple=True,
    type=click.Choice(ALL_SECTIONS),
    default=ALL_SECTIONS,
    show_default=True,
    help='Which section(s) to display. Repeat to select multiple.',
)
@click.option('--no-color', is_flag=True, help='Disable ANSI colours.')
def main(url: str, section: tuple[str, ...], no_color: bool) -> None:
    """Diagnostic report for the offsets-db PostgreSQL database."""

    if no_color:
        # Disable colour by making all codes empty strings.
        global RESET, BOLD, DIM, GREEN, YELLOW, RED, CYAN, BLUE
        RESET = BOLD = DIM = GREEN = YELLOW = RED = CYAN = BLUE = ''

    if not url:
        click.echo(
            'Error: no database URL. Set OFFSETS_DB_DATABASE_URL or pass --url.',
            err=True,
        )
        sys.exit(1)

    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)

    try:
        conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError as exc:
        click.echo(f'Connection failed: {exc}', err=True)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            for name in ALL_SECTIONS:
                if name in section:
                    SECTION_FNS[name](cur)
        click.echo()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
