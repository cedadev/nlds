"""Declare the SQLAlchemy ORM models for the NLDS Monitoring database"""
from sqlalchemy import Integer, String, Column, Enum, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

from nlds.rabbit.consumer import State


"""Declarative base class, containing the Metadata object"""
Base = declarative_base()

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
    # relationship for SubRecords (One to many)
    sub_records = relationship("SubRecord")


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
