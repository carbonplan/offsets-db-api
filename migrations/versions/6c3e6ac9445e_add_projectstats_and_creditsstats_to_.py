"""add projectstats and creditsstats to the database

Revision ID: 6c3e6ac9445e
Revises: 361d093e626c
Create Date: 2023-06-20 13:42:31.867840

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision = '6c3e6ac9445e'
down_revision = '361d093e626c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'creditstats',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('registry', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('transaction_type', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('total_credits', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_table(
        'projectstats',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('registry', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('total_projects', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('projectstats')
    op.drop_table('creditstats')
    # ### end Alembic commands ###