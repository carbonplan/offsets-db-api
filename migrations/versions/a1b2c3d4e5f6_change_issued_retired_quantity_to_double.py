"""change issued, retired, quantity columns to double precision

Revision ID: a1b2c3d4e5f6
Revises: 521f1619112c
Create Date: 2026-04-22

"""

import sqlalchemy as sa
from alembic import op

revision = 'a1b2c3d4e5f6'
down_revision = '521f1619112c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        'project',
        'issued',
        type_=sa.Double(),
        existing_type=sa.BigInteger(),
        postgresql_using='issued::double precision',
    )
    op.alter_column(
        'project',
        'retired',
        type_=sa.Double(),
        existing_type=sa.BigInteger(),
        postgresql_using='retired::double precision',
    )
    op.alter_column(
        'credit',
        'quantity',
        type_=sa.Double(),
        existing_type=sa.BigInteger(),
        existing_nullable=False,
        nullable=True,
        postgresql_using='quantity::double precision',
    )


def downgrade() -> None:
    op.alter_column(
        'credit',
        'quantity',
        type_=sa.BigInteger(),
        existing_type=sa.Double(),
        existing_nullable=True,
        nullable=False,
        postgresql_using='quantity::bigint',
    )
    op.alter_column(
        'project',
        'retired',
        type_=sa.BigInteger(),
        existing_type=sa.Double(),
        postgresql_using='retired::bigint',
    )
    op.alter_column(
        'project',
        'issued',
        type_=sa.BigInteger(),
        existing_type=sa.Double(),
        postgresql_using='issued::bigint',
    )
