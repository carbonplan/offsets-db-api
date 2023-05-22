"""add bi-directional relationships between project and credit with relationships

Revision ID: af213f65d057
Revises: 216d52435022
Create Date: 2023-05-22 17:13:39.123357

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision = 'af213f65d057'
down_revision = '216d52435022'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'credit',
        sa.Column('project_id', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('vintage', sa.Integer(), nullable=True),
        sa.Column('transaction_date', sa.Date(), nullable=True),
        sa.Column('transaction_type', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('details_url', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('recorded_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ['project_id'],
            ['project.project_id'],
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('credit')
    # ### end Alembic commands ###
