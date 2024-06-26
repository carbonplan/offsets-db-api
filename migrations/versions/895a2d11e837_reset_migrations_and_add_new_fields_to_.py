"""reset migrations and add new fields to project

Revision ID: 895a2d11e837
Revises:
Create Date: 2023-12-06 04:34:16.583574

"""

import sqlalchemy as sa
import sqlmodel
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '895a2d11e837'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'clip',
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('title', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('url', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('source', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('tags', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('notes', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('is_waybacked', sa.Boolean(), nullable=True),
        sa.Column('type', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'file',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('url', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('content_hash', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column(
            'status', sa.Enum('pending', 'success', 'failure', name='filestatus'), nullable=False
        ),
        sa.Column('error', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('recorded_at', sa.DateTime(), nullable=False),
        sa.Column(
            'category',
            sa.Enum('projects', 'credits', 'clips', 'unknown', name='filecategory'),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'project',
        sa.Column('project_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('name', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('registry', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('proponent', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('protocol', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('category', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('status', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('country', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('listed_at', sa.Date(), nullable=True),
        sa.Column('is_compliance', sa.Boolean(), nullable=True),
        sa.Column('retired', sa.BigInteger(), nullable=True),
        sa.Column('issued', sa.BigInteger(), nullable=True),
        sa.Column('first_issuance_at', sa.Date(), nullable=True),
        sa.Column('first_retirement_at', sa.Date(), nullable=True),
        sa.Column('project_url', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.PrimaryKeyConstraint('project_id'),
    )
    op.create_index(op.f('ix_project_project_id'), 'project', ['project_id'], unique=True)
    op.create_table(
        'clipproject',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('clip_id', sa.Integer(), nullable=False),
        sa.Column('project_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.ForeignKeyConstraint(
            ['clip_id'],
            ['clip.id'],
        ),
        sa.ForeignKeyConstraint(
            ['project_id'],
            ['project.project_id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'credit',
        sa.Column('quantity', sa.BigInteger(), nullable=True),
        sa.Column('vintage', sa.Integer(), nullable=True),
        sa.Column('transaction_date', sa.Date(), nullable=True),
        sa.Column('transaction_type', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.ForeignKeyConstraint(
            ['project_id'],
            ['project.project_id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_credit_project_id'), 'credit', ['project_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_credit_project_id'), table_name='credit')
    op.drop_table('credit')
    op.drop_table('clipproject')
    op.drop_index(op.f('ix_project_project_id'), table_name='project')
    op.drop_table('project')
    op.drop_table('file')
    op.drop_table('clip')
    # ### end Alembic commands ###
