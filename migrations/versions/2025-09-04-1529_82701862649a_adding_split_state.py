"""Adding SPLIT state

Revision ID: 82701862649a
Revises: 87bbd9e274a1
Create Date: 2025-09-04 15:29:03.369722

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from enum import Enum as PyEnum

# revision identifiers, used by Alembic.
revision = '82701862649a'
down_revision = '87bbd9e274a1'
branch_labels = None
depends_on = None


class OldState(PyEnum):
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
    SPLIT = 110
    # Initial state for searching for sub-states
    SEARCHING = 1000

# A map from the old values of State to the new ones, including doubling up on
# the catalog_restoring
state_map = {
    OldState.INITIALISING: NewState.INITIALISING,
    OldState.ROUTING: NewState.ROUTING,
    # PUT workflow states
    OldState.SPLITTING: NewState.SPLITTING,
    OldState.INDEXING: NewState.INDEXING,
    OldState.CATALOG_PUTTING: NewState.CATALOG_PUTTING,
    OldState.TRANSFER_PUTTING: NewState.TRANSFER_PUTTING,
    # GET workflow states
    OldState.CATALOG_GETTING: NewState.CATALOG_GETTING,
    OldState.ARCHIVE_GETTING: NewState.ARCHIVE_GETTING,
    OldState.TRANSFER_GETTING: NewState.TRANSFER_GETTING,
    OldState.TRANSFER_INIT: NewState.TRANSFER_INIT,
    OldState.ARCHIVE_INIT: NewState.ARCHIVE_INIT,
    OldState.ARCHIVE_PUTTING: NewState.ARCHIVE_PUTTING,
    OldState.ARCHIVE_PREPARING: NewState.ARCHIVE_GETTING,
    # CATALOG manipulation workflow states
    OldState.CATALOG_DELETING: NewState.CATALOG_DELETING,
    OldState.CATALOG_UPDATING: NewState.CATALOG_ARCHIVE_UPDATING,
    OldState.CATALOG_ARCHIVE_UPDATING: NewState.CATALOG_ARCHIVE_UPDATING,
    OldState.CATALOG_REMOVING: NewState.CATALOG_REMOVING,
    # Complete states
    OldState.COMPLETE: NewState.COMPLETE,
    OldState.FAILED: NewState.FAILED,
    OldState.COMPLETE_WITH_ERRORS: NewState.COMPLETE_WITH_ERRORS,
    OldState.COMPLETE_WITH_WARNINGS: NewState.COMPLETE_WITH_WARNINGS,
    # Initial state for searching for sub-states
    OldState.SEARCHING: NewState.SEARCHING

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
    NewState.TRANSFER_INIT: OldState.TRANSFER_INIT,
    NewState.ARCHIVE_INIT: OldState.ARCHIVE_INIT,
    NewState.ARCHIVE_PUTTING: OldState.ARCHIVE_PUTTING,
    NewState.ARCHIVE_PREPARING: OldState.ARCHIVE_GETTING,
    # CATALOG manipulation workflow states
    NewState.CATALOG_DELETING: OldState.CATALOG_DELETING,
    NewState.CATALOG_UPDATING: OldState.CATALOG_ARCHIVE_UPDATING,
    NewState.CATALOG_ARCHIVE_UPDATING: OldState.CATALOG_ARCHIVE_UPDATING,
    NewState.CATALOG_REMOVING: OldState.CATALOG_REMOVING,
    # Complete states
    NewState.COMPLETE: OldState.COMPLETE,
    NewState.FAILED: OldState.FAILED,
    NewState.COMPLETE_WITH_ERRORS: OldState.COMPLETE_WITH_ERRORS,
    NewState.COMPLETE_WITH_WARNINGS: OldState.COMPLETE_WITH_WARNINGS,
    NewState.SPLIT: OldState.COMPLETE,
    # Initial state for searching for sub-states
    NewState.SEARCHING: OldState.SEARCHING
}

# Create ENUM types, one for old, one for new
old_enum = sa.Enum(OldState, name="oldstate")
new_enum = sa.Enum(NewState, name="newstate")

def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()

def upgrade_catalog() -> None:
    pass

def downgrade_catalog() -> None:
    pass

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
        # cannot use ORM here as it will use the model definition of File.PathType,
        # which does not include all of the old enum values - revert to SQL
        # reverse dictionary lookup
        os = reverse_map[ns]
        cr = session.execute(
            sa.text(
                f"UPDATE sub_record SET old_state='{os.name}' WHERE state='{ns.name}'"
            )
        )
    # old_state now exists in each row with the correct enum value. we now have to
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
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

