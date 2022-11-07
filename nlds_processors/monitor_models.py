"""Declare the SQLAlchemy ORM models for the NLDS Monitoring database"""
from __future__ import annotations
import enum

from sqlalchemy import Integer, String, Column, Enum
from sqlalchemy.orm import declarative_base


"""Declarative base class, containing the Metadata object"""
Base = declarative_base()

class State(enum.Enum):
    ROUTING = 0
    SPLITTING = 1
    INDEXING = 2
    TRANSFER_PUTTING = 3
    CATALOG_PUTTING = 4
    CATALOG_GETTING = 5
    TRANSFER_GETTING = 6
    COMPLETE = 7
    FAILED = 8
    RETRYING = -1

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_
    
    @classmethod
    def has_name(cls, name):
        return name in cls._member_names_
    

class TransactionState(Base):
    """Class containing the details of the state of a transaction"""
    __tablename__ = "transaction_state"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the String of the UUID
    transaction_id = Column(String, nullable=False, index=True)
    # user who owns this holding
    user = Column(String, nullable=False)
    # group who owns this holding
    group = Column(String, nullable=False)
    # the furthest state reached by any subjobs, can be any of the State enums
    furthest_state = Column(Enum(State), nullable=False)
    # count of how many subjobs were created at the splitting/indexing steps 
    subjob_count = Column(Integer, nullable=False)
    # count of how many subjobs have reached each transaction state of a put 
    # workflow. Routing and splitting are not included as there will be no 
    # subjobs at that point.
    routing_count = Column(Integer)
    indexing_count = Column(Integer)
    transfer_putting_count = Column(Integer)
    catalog_putting_count = Column(Integer)
    complete_count = Column(Integer)
    failed_count = Column(Integer)
    # similarly for a get workflow - splitting doesn't yet happen in a GET but 
    # it may in the future so keeping these here for ease of migration
    catalog_getting_count = Column(Integer)
    transfer_getting_count = Column(Integer)
    retry_count = Column(Integer)



