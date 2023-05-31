"""add is_arb to the project table

Revision ID: 2e536c835001
Revises: af213f65d057
Create Date: 2023-05-25 16:55:06.954355

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '2e536c835001'
down_revision = 'af213f65d057'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('project', sa.Column('is_arb', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('project', 'is_arb')
    # ### end Alembic commands ###