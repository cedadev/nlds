"""add location.url

Revision ID: cdd0f6937b28
Revises: af796816f4f7
Create Date: 2023-07-10 16:53:58.852140

"""
from typing import List

from alembic import op
import sqlalchemy as sa
from sqlalchemy import (Integer, String, Column, DateTime, Enum, BigInteger, 
                        UniqueConstraint)
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, Session, declarative_base
from sqlalchemy.sql.expression import text

from nlds_processors.catalog.catalog_models import Storage
from nlds_processors.catalog.catalog_worker import CatalogConsumer
from nlds_processors.transferers.put_transfer import PutTransferConsumer
from nlds_processors.transferers.get_transfer import GetTransferConsumer
try:
    from nlds_processors.archiver.archive_get import GetArchiveConsumer
    from nlds_processors.archiver.archive_put import PutArchiveConsumer
except ModuleNotFoundError:
    PutArchiveConsumer = None
    GetArchiveConsumer = None
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = 'cdd0f6937b28'
down_revision = 'af796816f4f7'
branch_labels = None
depends_on = None

Base = declarative_base()
_DEFAULT_URL = "PLACEHOLDER_VALUE"

# recreating table declarations here so this revision isn't broken by importing 
# any future updates to these tables. 
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
    """Class containing details of a transaction.  Note that a holding can consist
    of many transactions."""
    __tablename__ = "transaction"
    # primay key / integer id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the String of the UUID
    transaction_id = Column(String, nullable=False, index=True)
    # date and time of ingest / adding to catalogue
    ingest_time = Column(DateTime)
    # relationship for files (One to many)
    aggregations = relationship("Aggregation", cascade="delete, delete-orphan")
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)


class Aggregation(Base):
    """Class containing details of an aggregation, equivalent to a sub-record in 
    the monitor but without state information. Note that a transaction can 
    consist of many aggregations, these exist as the appropriately sized 
    groupings of files for tape writing."""
    __tablename__ = "aggregation"
    # primay key / integer id
    id = Column(Integer, primary_key=True)
    # aggregation id - this will be the String of the UUID
    aggregation_id = Column(String, nullable=False, index=True)
    # relationship for files (One to many)
    files = relationship("File", cascade="delete, delete-orphan")
    # transaction id as ForeignKey "Parent"
    transaction_id = Column(Integer, ForeignKey("transaction.id"), 
                            index=True, nullable=False)


class File(Base):
    """Class containing the details of a single File"""
    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # aggregation_id as ForeignKey "Parent"
    aggregation_id = Column(Integer, ForeignKey("aggregation.id"), 
                            index=True, nullable=False)
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


class Location(Base):
    """Class containing the location on NLDS of a single File"""
    __tablename__ = "location"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # storage type = OBJECT_STORAGE | TAPE
    storage_type = Column(Enum(Storage))
    # The url associated with the location, i.e. the url of the tenancy on 
    # object storage and the tape server url on tape. 
    url = Column(String, nullable=False)
    # root of the file on the storage (bucket on object storage)
    root = Column(String, nullable=False)
    # path of the file on the storage
    path = Column(String, nullable=False)
    # last time the file was accessed
    access_time = Column(DateTime)
    # file id as ForeignKey "Parent"
    file_id = Column(Integer, ForeignKey("file.id"), 
                     index=True, nullable=False)


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



def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()


def find_default_url(consumer_priority_list: List, 
                     default_url: str = _DEFAULT_URL):
    """As we could be migrating the database from either a local development 
    environment or from a live or semi-live deployment, we should be checking 
    pretty thoroughly through consumer configs to find a value for the tenancy. 
    This function checks through a priority-ordered list of consumers for 
    default url information to use, as it's entirely possible to have been 
    missed off from where it should be (in the catalog_consumer) in a 
    #development environment. 
    """
    for consumer_type, tenancy_label in consumer_priority_list:
        # May not have required packages on machine so skip if it's not been 
        # imported properly
        if consumer_type is None:
            continue
        # Attempt to make the consumer
        consumer = consumer_type()
        try:
            # Attempt to extract the relevant url from the consumer config.
            url = consumer.consumer_config[tenancy_label]
        except KeyError:
            url = None
        # If we have found any specified value we exit the loop here.
        if url is not None:
            return url
    # Return the default otherwise.
    return default_url

def find_default_tenancy_url():
    """Wrapper around find_default_url with predefined consumer_priority_list 
    for tenancy finding."""
    consumer_priority_list = [
        (CatalogConsumer, "default_tenancy"),
        (PutTransferConsumer, "tenancy"),
        (GetTransferConsumer, "tenancy"),
        (PutArchiveConsumer, "tenancy"),
        (GetArchiveConsumer, "tenancy")
    ]
    return find_default_url(consumer_priority_list)

def find_default_tape_url():
    """Wrapper around find_default_url with predefined consumer_priority_list 
    for tenancy finding."""
    # Fewer places we can look in for the tape_url
    consumer_priority_list = [
        (CatalogConsumer, "default_tape_url"),
        (PutArchiveConsumer, "tape_url"),
        (GetArchiveConsumer, "tape_url")
    ]
    return find_default_url(consumer_priority_list)


def upgrade_catalog() -> None:
    # Try to find a relevant value defined in one of the consumer configs. 
    default_tenancy_url = find_default_tenancy_url()
    default_tape_url = find_default_tape_url()

    # Do initial schema migration with a placeholder value as default
    op.add_column('location', sa.Column('url', sa.String(), nullable=False, 
                                        server_default=text(_DEFAULT_URL)))
    
    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())

    # Get object store locations and change each url
    object_store_locs = session.query(Location).filter(
        Location.storage_type == Storage.OBJECT_STORAGE
    ).all()
    for loc in object_store_locs:
        loc.url = default_tenancy_url

    # Similarly now get all the tape locations and update each url
    tape_locs = session.query(Location).filter(
        Location.storage_type == Storage.TAPE
    ).all()
    for loc in tape_locs:
        loc.url = default_tape_url

    # Commit changes to db
    session.commit()

    # Here, we wrap in a batch context manager so SQLite can be migrated/altered 
    # too (see https://alembic.sqlalchemy.org/en/latest/batch.html for details)
    with op.batch_alter_table("location") as bop:
        # Remove the server_default from the url column. 
        bop.alter_column("url", server_default=None, 
                         existing_nullable=False, existing_type=sa.String())


def downgrade_catalog() -> None:
    # Start a batch alter to remove the url column
    with op.batch_alter_table("location") as bop:
        bop.drop_column('url')


def upgrade_monitor() -> None:
    pass


def downgrade_monitor() -> None:
    pass

