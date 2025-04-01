"""update_states

Revision ID: c7648694325f
Revises: f1f20ae58def
Create Date: 2024-12-16 16:05:21.356662

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from enum import Enum as PyEnum
from nlds_processors.monitor.monitor_models import SubRecord
from itertools import chain

# revision identifiers, used by Alembic.
revision = "c7648694325f"
down_revision = "f1f20ae58def"
branch_labels = None
depends_on = None


class OldState(PyEnum):
    # Generic states
    INITIALISING = -1
    ROUTING = 0
    COMPLETE = 100
    FAILED = 101
    # PUT workflow states
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    CATALOG_ROLLBACK = 5
    # GET workflow states
    CATALOG_GETTING = 10
    ARCHIVE_GETTING = 11
    TRANSFER_GETTING = 12
    # ARCHIVE_PUT workflow states
    ARCHIVE_INIT = 20
    CATALOG_ARCHIVE_AGGREGATING = 21
    ARCHIVE_PUTTING = 22
    CATALOG_ARCHIVE_UPDATING = 23
    # Shared ARCHIVE states
    CATALOG_ARCHIVE_ROLLBACK = 40


class NewState(PyEnum):
    # Generic states
    INITIALISING = -1
    ROUTING = 0
    # PUT workflow states
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    # GET workflow states
    CATALOG_GETTING = 10
    ARCHIVE_GETTING = 11
    TRANSFER_GETTING = 12
    TRANSFER_INIT = 13
    # ARCHIVE_PUT workflow states
    ARCHIVE_INIT = 20
    ARCHIVE_PUTTING = 21
    ARCHIVE_PREPARING = 22
    # CATALOG manipulation workflow states
    CATALOG_DELETING = 30
    CATALOG_UPDATING = 31
    CATALOG_ARCHIVE_UPDATING = 32
    CATALOG_REMOVING = 33
    # Complete states
    COMPLETE = 100
    FAILED = 101
    COMPLETE_WITH_ERRORS = 102
    COMPLETE_WITH_WARNINGS = 103
    # Initial state for searching for sub-states
    SEARCHING = 1000


# A map from the old values of State to the new ones, including doubling up on
# the catalog_restoring
state_map = {
    OldState.INITIALISING: NewState.INITIALISING,
    OldState.ROUTING: NewState.ROUTING,
    OldState.COMPLETE: NewState.COMPLETE,
    OldState.FAILED: NewState.FAILED,
    # PUT workflow states
    OldState.SPLITTING: NewState.SPLITTING,
    OldState.INDEXING: NewState.INDEXING,
    OldState.CATALOG_PUTTING: NewState.CATALOG_PUTTING,
    OldState.TRANSFER_PUTTING: NewState.TRANSFER_PUTTING,
    OldState.CATALOG_ROLLBACK: NewState.CATALOG_REMOVING,
    # GET workflow states
    OldState.CATALOG_GETTING: NewState.CATALOG_GETTING,
    OldState.ARCHIVE_GETTING: NewState.ARCHIVE_GETTING,
    OldState.TRANSFER_GETTING: NewState.TRANSFER_GETTING,
    # DEL workflow states
    # ARCHIVE_PUT workflow states
    OldState.ARCHIVE_INIT: NewState.ARCHIVE_INIT,
    OldState.CATALOG_ARCHIVE_AGGREGATING: NewState.TRANSFER_INIT,
    OldState.ARCHIVE_PUTTING: NewState.ARCHIVE_PUTTING,
    OldState.CATALOG_ARCHIVE_UPDATING: NewState.CATALOG_ARCHIVE_UPDATING,
    # Shared ARCHIVE states
    OldState.CATALOG_ARCHIVE_ROLLBACK: NewState.CATALOG_ARCHIVE_UPDATING,
}

# reverse mapping of above for downgrade
reverse_map = {
    NewState.INITIALISING: OldState.INITIALISING,
    NewState.ROUTING: OldState.ROUTING,
    # PUT workflow states
    NewState.SPLITTING: OldState.SPLITTING,
    NewState.INDEXING: OldState.INDEXING,
    NewState.CATALOG_PUTTING: OldState.CATALOG_PUTTING,
    NewState.TRANSFER_PUTTING: OldState.TRANSFER_PUTTING,
    # GET workflow states
    NewState.CATALOG_GETTING: OldState.CATALOG_GETTING,
    NewState.ARCHIVE_GETTING: OldState.ARCHIVE_GETTING,
    NewState.TRANSFER_GETTING: OldState.TRANSFER_GETTING,
    NewState.TRANSFER_INIT: OldState.INITIALISING,
    NewState.ARCHIVE_INIT: OldState.ARCHIVE_INIT,
    NewState.ARCHIVE_PUTTING: OldState.ARCHIVE_PUTTING,
    NewState.ARCHIVE_PREPARING: OldState.ARCHIVE_GETTING,
    # CATALOG manipulation workflow states
    NewState.CATALOG_DELETING: OldState.CATALOG_ROLLBACK,
    NewState.CATALOG_UPDATING: OldState.CATALOG_ARCHIVE_UPDATING,
    NewState.CATALOG_ARCHIVE_UPDATING: OldState.CATALOG_ARCHIVE_UPDATING,
    NewState.CATALOG_REMOVING: OldState.CATALOG_ROLLBACK,
    # Complete states
    NewState.COMPLETE: OldState.COMPLETE,
    NewState.FAILED: OldState.FAILED,
    NewState.COMPLETE_WITH_ERRORS: OldState.COMPLETE,
    NewState.COMPLETE_WITH_WARNINGS: OldState.COMPLETE,
    # Initial state for searching for sub-states
    NewState.SEARCHING: OldState.INITIALISING
}

# clash CATALOG_ARCHIVE_AGGREGATING = 21 (old),
#       ARCHIVE_PUTTING = 21 (new)
# clash ARCHIVE_PUTTING = 22 (old),
#       ARCHIVE_PREPARING = 22 (new),

# Create ENUM types, one for old, one for new
old_enum = sa.Enum(OldState, name="oldstate")
new_enum = sa.Enum(NewState, name="newstate")


def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()


def upgrade_catalog() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_catalog() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def upgrade_monitor() -> None:
    # create new enum
    new_enum.create(op.get_bind(), checkfirst=False)
    # add new_state column
    with op.batch_alter_table("sub_record") as bop:
        bop.add_column(
            sa.Column(
                "new_state",
                sa.Enum(NewState),
                nullable=False,
                server_default=NewState.INITIALISING.name,
                default=NewState.INITIALISING.name,
            ),
        )

    # Get the session to update the database values with
    session = Session(bind=op.get_bind())

    # Loop through each of the states in the map from old states to new states.
    for os in OldState:
        # cannot use ORM here as it will use the model definition of SubRecord.State,
        # which does not include all of the old enum values - revert to SQL
        ns = state_map[os]
        cr = session.execute(
            sa.text(
                f"UPDATE sub_record SET new_state='{ns.name}' WHERE state='{os.name}'"
            )
        )
    # new_state now exists in each row with the correct enum value. we now have to
    # 1. Delete the old state field
    # 2. Delete the old enum
    # 3. Rename the new enum
    # 4. Rename the new state field

    with op.batch_alter_table("sub_record") as bop:
        bop.drop_column("state")
    # Remove the old enum type
    op.execute(sa.text("DROP TYPE state"))

    # Rename the new enum
    session.execute(sa.text("ALTER TYPE newstate RENAME to state"))
    with op.batch_alter_table("sub_record") as bop:
        bop.alter_column(
            "new_state",
            existing_server_default=None,
            existing_nullable=False,
            type="state",
            new_column_name="state",
        )

    # Commit the changes to the db.
    session.commit()


def downgrade_monitor() -> None:
    # create old enum
    old_enum.create(op.get_bind(), checkfirst=False)
    # add old_state column
    with op.batch_alter_table("sub_record") as bop:
        bop.add_column(
            sa.Column(
                "old_state",
                sa.Enum(OldState),
                nullable=False,
                server_default=OldState.INITIALISING.name,
                default=OldState.INITIALISING.name,
            ),
        )
    # Get the session to update the database values with
    session = Session(bind=op.get_bind())

    # Loop through each of the states in the map from new states to old states.
    for ns in NewState:
        # cannot use ORM here as it will use the model definition of SubRecord.State,
        # which does not include all of the old enum values - revert to SQL
        # reverse dictionary lookup
        os = reverse_map[ns]
        cr = session.execute(
            sa.text(
                f"UPDATE sub_record SET old_state='{os.name}' WHERE state='{ns.name}'"
            )
        )
    # new_state now exists in each row with the correct enum value. we now have to
    # 1. Delete the state field
    # 2. Delete the enum
    # 3. Rename the old enum
    # 4. Rename the old state field

    with op.batch_alter_table("sub_record") as bop:
        bop.drop_column("state")
    # Remove the old enum type
    op.execute(sa.text("DROP TYPE state"))

    # Rename the old enum
    session.execute(sa.text("ALTER TYPE oldstate RENAME to state"))
    with op.batch_alter_table("sub_record") as bop:
        bop.alter_column(
            "old_state",
            existing_server_default=None,
            existing_nullable=False,
            type="state",
            new_column_name="state",
        )

    # Commit the changes to the db.
    session.commit()
