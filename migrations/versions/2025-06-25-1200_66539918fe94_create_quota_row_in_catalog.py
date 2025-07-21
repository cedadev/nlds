"""create quota row in catalog

Revision ID: 66539918fe94
Revises: 87bbd9e274a1
Create Date: 2025-06-25 12:00:48.246801

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '66539918fe94'
down_revision = '87bbd9e274a1'
branch_labels = None
depends_on = None


def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()





def upgrade_catalog() -> None:
    # Add quota table
    op.create_table(
        'quota',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('group', sa.String(), nullable=True),
        sa.Column('size', sa.Integer(), nullable=True),
        sa.Column('used', sa.Integer(), nullable=True)

    )


def downgrade_catalog() -> None:
    op.drop_table('quota')


def upgrade_monitor() -> None:
    pass


def downgrade_monitor() -> None:
    pass

