"""add transaction_serial_number to the credit model

Revision ID: 8dafad959469
Revises: 2e536c835001
Create Date: 2023-05-25 17:03:46.987689

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision = '8dafad959469'
down_revision = '2e536c835001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        'credit',
        sa.Column('transaction_serial_number', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('credit', 'transaction_serial_number')
    # ### end Alembic commands ###