"""add failed_fl to aggregations

Revision ID: f1f20ae58def
Revises: eaeeec0b5cf2
Create Date: 2023-10-05 22:00:51.401133

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import expression


# revision identifiers, used by Alembic.
revision = 'f1f20ae58def'
down_revision = 'eaeeec0b5cf2'
branch_labels = None
depends_on = None

######
# No need to declare all the objects as we're not using the ORM to make any 
# changes. 

def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()



def upgrade_catalog() -> None:
    # Generate the new column with default value False. 
    op.add_column('aggregation', sa.Column('failed_fl', sa.Boolean(),
                                            nullable=False, default=False,
                                            server_default=expression.false()))
    # No need to do a data migration as there's no way to tell whether an agg 
    # has failed purely from the database in its current form. We have to assume 
    # all are fine and manually update the db if it's clear one has failed.


def downgrade_catalog() -> None:
    # As straightforward as just removing the column 
    with op.batch_alter_table("aggregation") as bop:
        bop.drop_column('failed_fl')


def upgrade_monitor() -> None:
    pass

def downgrade_monitor() -> None:
    pass

