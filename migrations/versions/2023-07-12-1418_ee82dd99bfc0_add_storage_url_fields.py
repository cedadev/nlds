"""add storage url fields

Revision ID: ee82dd99bfc0
Revises: bf09ce0d6899
Create Date: 2023-07-12 14:18:22.105227

"""
from typing import List

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Integer, String, Column, DateTime, Enum, BigInteger, UniqueConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.sql.expression import text

from nlds_processors.catalog.catalog_models import Storage
from nlds_processors.catalog.catalog_worker import CatalogConsumer
from nlds_processors.transfer.put_transfer import PutTransferConsumer
from nlds_processors.transfer.get_transfer import GetTransferConsumer
# Import archive consumers, some of which import xrootd and may break on 
# unprepared environments
from nlds_processors.archive.s3_to_tarfile_tape import S3ToTarfileTape
from nlds_processors.archive.archive_base import ArchiveError
try:
    from nlds_processors.archive.archive_get import GetArchiveConsumer
    from nlds_processors.archive.archive_put import PutArchiveConsumer
except ModuleNotFoundError:
    PutArchiveConsumer = None
    GetArchiveConsumer = None
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = 'ee82dd99bfc0'
down_revision = 'bf09ce0d6899'
branch_labels = None
depends_on = None

# recreating table declarations here so this revision isn't broken by importing 
# future updates to these tables. Use a separate base from the one used by the 
# catalog
Base = declarative_base()
_DEFAULT_URL = "placeholder"

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
    # scheme / protocol from the url 
    url_scheme = Column(String, nullable=False)
    # network location from the url
    url_netloc = Column(String, nullable=False)
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

def find_default_tape_url():
    """Wrapper around find_default_url with predefined consumer_priority_list 
    for tenancy finding."""
    # Fewer places we can look in for the tape_url
    consumer_priority_list = [
        (CatalogConsumer, "default_tape_url"),
        (PutArchiveConsumer, "tape_url"),
        (GetArchiveConsumer, "tape_url"),
    ]
    tape_url = find_default_url(consumer_priority_list)
    try: 
        netloc = "/".join(S3ToTarfileTape._split_tape_url(tape_url))
    except ArchiveError:
        netloc = tape_url
    return "root", netloc

def find_default_tenancy_url():
    """Wrapper around find_default_url with predefined consumer_priority_list 
    for tenancy finding."""
    consumer_priority_list = [
        (CatalogConsumer, "default_tenancy"),
        (PutTransferConsumer, "tenancy"),
        (GetTransferConsumer, "tenancy"),
        (PutArchiveConsumer, "tenancy"),
        (GetArchiveConsumer, "tenancy"),
    ]
    return "http", find_default_url(consumer_priority_list)


def upgrade_catalog() -> None:
    # Try to find a relevant value defined in one of the consumer configs. 
    _tenancy_scheme, _tenancy_netloc = find_default_tenancy_url()
    _tape_scheme, _tape_netloc = find_default_tape_url()

    # Do initial schema migration with a placeholder value as default
    op.add_column('location', sa.Column('url_scheme', sa.String(), 
                                        nullable=True,
                                        # server_default=text(_DEFAULT_URL)
                                        ))
    op.add_column('location', sa.Column('url_netloc', sa.String(), 
                                        nullable=True,
                                        # server_default=text(_DEFAULT_URL)
                                        ))
    
    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())

    # Get object store locations and change each url
    objstore_locs = session.query(Location).filter(
        Location.storage_type == Storage.OBJECT_STORAGE
    ).all()
    for loc in objstore_locs:
        loc.url_netloc = _tenancy_netloc
        loc.url_scheme = _tenancy_scheme

    # Similarly now get all the tape locations and update each url
    tape_locs = session.query(Location).filter(
        Location.storage_type == Storage.TAPE
    ).all()
    for loc in tape_locs:
        loc.url_netloc = _tape_netloc
        loc.url_scheme = _tape_scheme

    # Commit changes to db
    session.commit()

    # Here, we wrap in a batch context manager so SQLite can be migrated/altered 
    # too (see https://alembic.sqlalchemy.org/en/latest/batch.html for details)
    with op.batch_alter_table("location") as bop:
        # Remove the server_default from both new columns. 
        bop.alter_column("url_scheme", existing_server_default=None,
                         nullable=False, existing_type=sa.String())
        bop.alter_column("url_netloc", existing_server_default=None,
                         nullable=False, existing_type=sa.String())


def downgrade_catalog() -> None:
    # Start a batch alter to remove the url columns
    with op.batch_alter_table("location") as bop:
        bop.drop_column('url_netloc')
        bop.drop_column('url_scheme')


def upgrade_monitor() -> None:
    pass


def downgrade_monitor() -> None:
    pass

