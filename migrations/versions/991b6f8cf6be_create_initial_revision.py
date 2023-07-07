"""create initial revision

Revision ID: 991b6f8cf6be
Revises: 
Create Date: 2023-07-05 17:28:44.244493

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '991b6f8cf6be'
down_revision = None
branch_labels = None
depends_on = None


def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()





def upgrade_catalog() -> None:
    pass


def downgrade_catalog() -> None:
    pass


def upgrade_monitor() -> None:
    pass


def downgrade_monitor() -> None:
    pass

