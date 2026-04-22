"""add protocol_unassigned column to project

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


def downgrade() -> None:
    op.drop_index(
        'ix_project_protocol_unassigned_gin',
        table_name='project',
        postgresql_using='gin',
    )
    op.drop_column('project', 'protocol_unassigned')
