"""create initial revision

Revision ID: 991b6f8cf6be
Revises: 
Create Date: 2023-07-05 17:28:44.244493

"""
from enum import Enum as PyEnum

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Integer, String, Column, DateTime, Enum, BigInteger, UniqueConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.sql import func

from nlds_processors.catalog.catalog_models import Storage
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = '991b6f8cf6be'
down_revision = None
branch_labels = None
depends_on = None


CatalogBase = declarative_base()

class Holding(CatalogBase):
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


class Transaction(CatalogBase):
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


class Tag(CatalogBase):
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


class File(CatalogBase):
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


class Location(CatalogBase):
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


class Checksum(CatalogBase):
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


MonitorBase = declarative_base()

class State(PyEnum):
    INITIALISING = -1
    ROUTING = 0
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    CATALOG_ROLLBACK = 5
    CATALOG_GETTING = 6
    TRANSFER_GETTING = 7
    COMPLETE = 8
    FAILED = 9
    CATALOG_BACKUP = 10     # This is essenitally the policy control step
    ARCHIVE_PUTTING = 11
    CATALOG_RESTORING = 12
    ARCHIVE_GETTING = 13
    CATALOG_UPDATING = 14

class TransactionRecord(MonitorBase):
    """Class containing the details of the state of a transaction"""
    __tablename__ = "transaction_record"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the String of the UUID
    transaction_id = Column(String, nullable=False, index=True)
    # user who owns this holding
    user = Column(String, nullable=False)
    # group who owns this holding
    group = Column(String, nullable=False)
    # job label - optional job label for PUT and GET
    job_label = Column(String, nullable=True)
    # defining api-action invoked to start the transaction
    api_action = Column(String, nullable=False)
    # Time of initial submission
    creation_time = Column(DateTime, default=func.now())
    # relationship for SubRecords (One to many)
    sub_records = relationship("SubRecord")
    # relationship for Warnings (One to many)
    warnings = relationship("Warning", cascade="delete, delete-orphan")

class SubRecord(MonitorBase):
    __tablename__ = "sub_record"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # subrecord id - this will be the String of the UUID of the sub record
    sub_id = Column(String, nullable=False, index=True)
    # the furthest state reached by any subjobs, can be any of the State enums
    state = Column(Enum(State), nullable=False)
    # count of how many times the subrecord has been retried
    retry_count = Column(Integer, nullable=False)
    # timestamp of last update
    last_updated = Column(DateTime, nullable=False, default=func.now(), 
                          onupdate=func.now())
    # relationship for failed files (zero to many)
    failed_files = relationship("FailedFile")

    # transaction_record_id as ForeignKey
    transaction_record_id = Column(Integer, ForeignKey("transaction_record.id"), 
                                   index=True, nullable=False)

class FailedFile(MonitorBase):
    __tablename__ = "failed_file"

    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # filepath of failed item
    filepath = Column(String)
    # final reason for failure
    reason = Column(String)
    # sub_record_id as ForeignKey
    sub_record_id = Column(Integer, ForeignKey("sub_record.id"), 
                           index=True, nullable=False)

class Warning(MonitorBase):
    __tablename__ = "warning"

    # just two columns - primary key and warning string
    id = Column(Integer, primary_key=True)
    warning = Column(String)
    # link to transaction record warning about
    transaction_record_id = Column(Integer, ForeignKey("transaction_record.id"), 
                                   index=True, nullable=False)



def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()





def upgrade_catalog() -> None:
    # Attempt to create all tables for the catalog
    session = Session(bind=op.get_bind())
    try:
        CatalogBase.metadata.create_all(session.get_bind()) 
    except Exception as e:
        session.rollback()
    else:
        session.commit()


def downgrade_catalog() -> None:
    # No need to delete anything 
    pass


def upgrade_monitor() -> None:
    # Attempt to create all tables for the monitor
    session = Session(bind=op.get_bind())
    try:
        MonitorBase.metadata.create_all(session.get_bind())
    except Exception as e:
        session.rollback()
    else:
        session.commit()


def downgrade_monitor() -> None:
    # No need to delete anything 
    pass

