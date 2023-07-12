"""add aggregation table

Revision ID: bf09ce0d6899
Revises: 991b6f8cf6be
Create Date: 2023-07-12 12:38:26.906605

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Integer, String, Column, DateTime, Enum, BigInteger, UniqueConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship

from nlds_processors.catalog.catalog_models import Storage
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = 'bf09ce0d6899'
down_revision = '991b6f8cf6be'
branch_labels = None
depends_on = None


# recreating table declarations here so this revision isn't broken by importing 
# future updates to these tables. Use a separate base from the one used by the 
# catalog
Base = declarative_base()

class Holding(Base):
    """Class containing the details of a Holding - i.e. a batch"""
    __tablename__ = "holding"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # label - either supplied by user or derived from first transaction_id
    label = Column(String, nullable=False, index=True)
    # user who owns this holding
    user = Column(String, nullable=False)
    # group who owns this holding
    group = Column(String, nullable=False)
    # relationship for tags (One to many)
    tags = relationship("Tag", backref="holding", cascade="delete, delete-orphan")
    # relationship for transactions (One to many)
    transactions = relationship("Transaction", cascade="delete, delete-orphan")
    # label must be unique per user
    __table_args__ = (UniqueConstraint('label', 'user'),)
    # return the tags as a dictionary
    def get_tags(self):
        tags = {}
        for t in self.tags:
            tags[t.key] = t.value
        return tags
    # return the transaction ids as a list
    def get_transaction_ids(self):
        t_ids = []
        for t in self.transactions:
            t_ids.append(t.transaction_id)
        return t_ids


class Transaction(Base):
    """Class containing details of a transaction.  Note that a holding can 
    consist of many transactions."""
    __tablename__ = "transaction"
    # primay key / integer id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the String of the UUID
    transaction_id = Column(String, nullable=False, index=True)
    # date and time of ingest / adding to catalogue
    ingest_time = Column(DateTime)
    # relationship for files (One to many)
    files = relationship("File", cascade="delete, delete-orphan")
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)


class Tag(Base):
    """Class containing the details of a Tag that can be assigned to a Holding"""
    __tablename__ = "tag"
    # primary key
    id = Column(Integer, primary_key=True)
    # key:value tags - key should be unique per holding_id
    key = Column(String)
    value = Column(String)
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)

    __table_args__ = (UniqueConstraint('key', 'holding_id'),)


class File(Base):
    """Class containing the details of a single File"""
    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # transaction id as ForeignKey "Parent"
    transaction_id = Column(Integer, ForeignKey("transaction.id"), 
                            index=True, nullable=False)
    # original path on POSIX disk - this should be unique per holding
    original_path = Column(String)
    # PathType, same as the nlds.details.PathType enum
    path_type = Column(Enum(PathType))
    # path to the link
    link_path = Column(String)
    # file size, in bytes
    size = Column(BigInteger)
    # user name file belongs to
    user = Column(Integer)
    # user group file belongs to
    group = Column(Integer)
    # unix style file permissions
    file_permissions = Column(Integer)

    # relationship for location (one to many)
    location = relationship("Location", cascade="delete, delete-orphan")
    # relationship for checksum (one to one)
    checksum = relationship("Checksum", cascade="delete, delete-orphan")


class Location(Base):
    """Class containing the location on NLDS of a single File"""
    __tablename__ = "location"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # storage type = OBJECT_STORAGE | TAPE
    storage_type = Column(Enum(Storage))
    # root of the file on the storage (bucket on object storage)
    root = Column(String, nullable=False)
    # path of the file on the storage
    path = Column(String, nullable=False)
    # last time the file was accessed
    access_time = Column(DateTime)
    # file id as ForeignKey "Parent"
    file_id = Column(Integer, ForeignKey("file.id"), 
                     index=True, nullable=False)
    aggregation_id = Column(Integer, ForeignKey("aggregation.id"), 
                            index=True, nullable=True)


class Checksum(Base):
    """Class containing checksum and algorithm used to calculate checksum"""
    __tablename__ = "checksum"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # checksum
    checksum = Column(String, nullable=False)
    # checksum method / algorithm
    algorithm = Column(String, nullable=False)
    # file id as ForeignKey "Parent" (one to many)
    file_id = Column(Integer, ForeignKey("file.id"), 
                     index=True, nullable=False)
    # checksum must be unique per algorithm
    __table_args__ = (UniqueConstraint('checksum', 'algorithm'),)


class Aggregation(Base):
    """Class containing the details of file aggregations made for writing files 
    to tape (specifically CTA) as tars"""
    __tablename__ = "aggregation"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # The name of the tarfile on tape
    tarname = Column(String, nullable=False)
    # checksum
    checksum = Column(String, nullable=False)
    # checksum method / algorithm
    algorithm = Column(String, nullable=False)

    # relationship for location (one to many)
    location = relationship("Location", cascade="delete, delete-orphan")



def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()





def upgrade_catalog() -> None:
    # Schema migration - add aggregation table and aggregation_id column to 
    # location
    op.create_table(
        'aggregation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('tarname', sa.String(), nullable=False),
        sa.Column('checksum', sa.String(), nullable=False),
        sa.Column('algorithm', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.add_column('location', sa.Column('aggregation_id', sa.Integer(), 
                                        nullable=True))

    # Next to data migration, but we're adding an empty table so no need in this
    # revision. 
    # NOTE: There shouldn't be, but there could be tape locations which have yet 
    # to have aggregations applied. Not sure if we need to, or even can, do 
    # anything about those, seems very unlikely

    # Finally complete the schema migration with indices and foreign keys
    op.create_index(op.f('ix_location_aggregation_id'), 'location', 
                    ['aggregation_id'], unique=False)
    # We wrap it in a batch context manager so SQLite can be migrated/altered 
    # too (see https://alembic.sqlalchemy.org/en/latest/batch.html for details)
    # Here we also specify a naming convention so that the by-default-unnamed 
    # foreign key constraints can be added/removed. This will need to be copied 
    # to all future revisions.
    naming_conv = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    }
    with op.batch_alter_table("location", naming_convention=naming_conv) as bop:
        # Create a new foreign key constraint according to naming convention 
        bop.create_foreign_key("fk_locations_aggregation_ids_aggregations", 
                               'aggregation', ['aggregation_id'], ['id'])


def downgrade_catalog() -> None:
    # No data migration to do so just drop all the things created in the upgrade
    op.drop_index(op.f('ix_location_aggregation_id'), table_name='location')
    # Use a batch alter table command as above.
    with op.batch_alter_table("location") as bop:
        # Here we don't need to delete the foreign key constraint because we are 
        # dropping the column
        bop.drop_column('aggregation_id')
    # Finally drop the aggregations table
    op.drop_table('aggregation')


def upgrade_monitor() -> None:
    pass


def downgrade_monitor() -> None:
    pass

