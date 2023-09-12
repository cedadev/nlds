"""update state enum vals for archive workflows

Revision ID: eaeeec0b5cf2
Revises: fa9f4b687d19
Create Date: 2023-08-28 11:59:54.203864

"""
from enum import Enum as PyEnum
from itertools import chain

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Integer, String, Column, DateTime, Enum
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import text


# revision identifiers, used by Alembic.
revision = 'eaeeec0b5cf2'
down_revision = 'fa9f4b687d19'
branch_labels = None
depends_on = None

"""Declarative base class, containing the Metadata object"""
Base = declarative_base()

# Explicitly declare the old and new State enums here so the migration still 
# works at future, possibly different, commits.
class OldState(PyEnum):
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

class NewState(PyEnum):
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

class State(PyEnum):
    """Combination of old and new, favouring new numbers"""
    _ignore_ = 'member cls'
    cls = vars()
    for member in chain(list(NewState), list(OldState)):
        if member.name not in cls:
            cls[member.name] = member.value


class TransactionRecord(Base):
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

class SubRecord(Base):
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

class FailedFile(Base):
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

class Warning(Base):
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
    pass


def downgrade_catalog() -> None:
    pass

# A map from the old values of State to the new ones, including doubling up on 
# the catalog_restoring
state_map = {
    OldState.ROUTING: NewState.ROUTING,
    OldState.INITIALISING: NewState.INITIALISING,
    OldState.COMPLETE: NewState.COMPLETE,
    OldState.FAILED: NewState.FAILED,
    OldState.SPLITTING: NewState.SPLITTING,
    OldState.INDEXING: NewState.INDEXING,
    OldState.CATALOG_PUTTING: NewState.CATALOG_PUTTING,
    OldState.TRANSFER_PUTTING: NewState.TRANSFER_PUTTING,
    OldState.CATALOG_ROLLBACK: NewState.CATALOG_ROLLBACK,
    OldState.CATALOG_GETTING: NewState.CATALOG_GETTING,
    OldState.ARCHIVE_GETTING: NewState.ARCHIVE_GETTING,
    OldState.TRANSFER_GETTING: NewState.TRANSFER_GETTING,
    OldState.CATALOG_BACKUP: NewState.CATALOG_ARCHIVE_AGGREGATING,
    OldState.ARCHIVE_PUTTING: NewState.ARCHIVE_PUTTING,
    OldState.CATALOG_RESTORING: NewState.CATALOG_ARCHIVE_ROLLBACK,
    OldState.CATALOG_UPDATING: NewState.CATALOG_ARCHIVE_UPDATING,
}

def upgrade_monitor() -> None:
    # No schema changes to make for this upgrade.

    # Get the session to update the database values with 
    session = Session(bind=op.get_bind())
    # Loop through each of the states in the map from old states to new states. 
    for old_state, new_state in state_map.items():
        sub_records = session.query(SubRecord).filter(
            SubRecord.state.name == old_state.name
        ).all()
        # Assing the state to the new value
        for sr in sub_records:
            sr.state = new_state
        
    # Commit the changes to the db.  
    session.commit()

def downgrade_monitor() -> None:
    # No schema changes to make for this downgrade either.

    # Get the session to update the database values with 
    session = Session(bind=op.get_bind())
    # Loop through each of the states in the map from old states to new states. 
    for old_state, new_state in state_map.items():
        sub_records = session.query(SubRecord).filter(
            SubRecord.state.name == new_state.name
        ).all()
        # Assing the state to the old value. 
        for sr in sub_records:
            sr.state = old_state
        
    # Commit the changes to the db.  
    session.commit()

