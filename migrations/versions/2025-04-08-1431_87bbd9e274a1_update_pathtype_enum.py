"""Update PathType enum

Revision ID: 87bbd9e274a1
Revises: c7648694325f
Create Date: 2025-04-08 14:31:36.182005

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session

from enum import Enum as PyEnum


# revision identifiers, used by Alembic.
revision = "87bbd9e274a1"
down_revision = "c7648694325f"
branch_labels = None
depends_on = None


class OldPathType(PyEnum):
    FILE = 0
    DIRECTORY = 1
    LINK_UNCLASSIFIED = 2
    LINK_COMMON_PATH = 3
    LINK_ABSOLUTE_PATH = 4
    NOT_RECOGNISED = 5


class NewPathType(PyEnum):
    FILE = 0
    DIRECTORY = 1
    LINK = 2
    NOT_RECOGNISED = 5
    UNINDEXED = 6


path_type_map = {
    OldPathType.FILE: NewPathType.FILE,
    OldPathType.DIRECTORY: NewPathType.DIRECTORY,
    OldPathType.LINK_UNCLASSIFIED: NewPathType.LINK,
    OldPathType.LINK_COMMON_PATH: NewPathType.LINK,
    OldPathType.LINK_ABSOLUTE_PATH: NewPathType.LINK,
    OldPathType.NOT_RECOGNISED: NewPathType.NOT_RECOGNISED,
}

path_type_reverse_map = {
    NewPathType.FILE: OldPathType.FILE,
    NewPathType.DIRECTORY: OldPathType.DIRECTORY,
    NewPathType.LINK: OldPathType.LINK_COMMON_PATH,
    NewPathType.NOT_RECOGNISED: OldPathType.NOT_RECOGNISED,
}

old_enum = sa.Enum(OldPathType, name="oldpathtype")
new_enum = sa.Enum(NewPathType, name="newpathtype")


def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()


def upgrade_catalog() -> None:
    # create new enum
    new_enum.create(op.get_bind(), checkfirst=False)
    # add new_pathtype column
    with op.batch_alter_table("file") as bop:
        bop.add_column(
            sa.Column(
                "new_pathtype",
                sa.Enum(NewPathType),
                nullable=False,
                server_default=NewPathType.NOT_RECOGNISED.name,
                default=NewPathType.NOT_RECOGNISED.name,
            ),
        )
    # Get the session to update the database values with
    session = Session(bind=op.get_bind())

    # Loop through each of the path_types in the map from old_pathtype to new_pathtype.
    for opt in OldPathType:
        # cannot use ORM here as it will use the model definition of File.PathType,
        # which does not include all of the old enum values - revert to SQL
        npt = path_type_map[opt]
        cr = session.execute(
            sa.text(
                f"UPDATE file SET new_pathtype='{npt.name}' WHERE path_type='{opt.name}'"
            )
        )
    # new_pathtype now exists in each row with the correct enum value. we now have to
    # 1. Delete the old pathtype field
    # 2. Delete the old enum
    # 3. Rename the new enum
    # 4. Rename the new pathtype field

    with op.batch_alter_table("file") as bop:
        bop.drop_column("path_type")
    # Remove the old enum type
    op.execute(sa.text("DROP TYPE pathtype"))

    # Rename the new enum
    session.execute(sa.text("ALTER TYPE newpathtype RENAME to pathtype"))
    with op.batch_alter_table("file") as bop:
        bop.alter_column(
            "new_pathtype",
            existing_server_default=None,
            existing_nullable=False,
            type="pathtype",
            new_column_name="path_type",
        )

    # Commit the changes to the db.
    session.commit()


def downgrade_catalog() -> None:
    # do the above - in reverse
    # create old enum
    old_enum.create(op.get_bind(), checkfirst=False)
    # add old_pathtype column
    with op.batch_alter_table("file") as bop:
        bop.add_column(
            sa.Column(
                "old_pathtype",
                sa.Enum(OldPathType),
                nullable=False,
                server_default=OldPathType.NOT_RECOGNISED.name,
                default=OldPathType.NOT_RECOGNISED.name,
            ),
        )
    # Get the session to update the database values with
    session = Session(bind=op.get_bind())

    # Loop through each of the path_types in the map from new path_types to old path_types.
    for npt in NewPathType:
        # cannot use ORM here as it will use the model definition of File.PathType,
        # which does not include all of the old enum values - revert to SQL
        # reverse dictionary lookup
        opt = path_type_reverse_map[npt]
        cr = session.execute(
            sa.text(
                f"UPDATE file SET old_pathtype='{opt.name}' WHERE path_type='{npt.name}'"
            )
        )
    # old_pathtype now exists in each row with the correct enum value. we now have to
    # 1. Delete the path_type field
    # 2. Delete the enum
    # 3. Rename the old enum
    # 4. Rename the old_pathtype field

    with op.batch_alter_table("file") as bop:
        bop.drop_column("path_type")
    # Remove the old enum type
    op.execute(sa.text("DROP TYPE pathtype"))

    # Rename the old enum
    session.execute(sa.text("ALTER TYPE oldpathtype RENAME to path_type"))
    with op.batch_alter_table("file") as bop:
        bop.alter_column(
            "old_pathtype",
            existing_server_default=None,
            existing_nullable=False,
            type="pathtype",
            new_column_name="path_type",
        )

    # Commit the changes to the db.
    session.commit()


def upgrade_monitor() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_monitor() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
