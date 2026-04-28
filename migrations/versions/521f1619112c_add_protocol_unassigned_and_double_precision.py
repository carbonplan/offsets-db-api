"""add protocol_unassigned column and change issued/retired/quantity to double precision

Revision ID: 521f1619112c
Revises: e7d9d6cf54c6
Create Date: 2026-04-22

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '521f1619112c'
down_revision = 'e7d9d6cf54c6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'project',
        sa.Column('protocol_unassigned', postgresql.ARRAY(sa.String()), nullable=True),
    )
    op.create_index(
        'ix_project_protocol_unassigned_gin',
        'project',
        ['protocol_unassigned'],
        unique=False,
        postgresql_using='gin',
    )
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
    op.drop_index(
        'ix_project_protocol_unassigned_gin',
        table_name='project',
        postgresql_using='gin',
    )
    op.drop_column('project', 'protocol_unassigned')
