"""Declare the SQLAlchemy ORM models for the NLDS Catalog database"""
from __future__ import annotations

from sqlalchemy import Integer, String, Column, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

"""Declarative base class, containing the Metadata object"""
Base = declarative_base()

class Holding(Base):
    """Class containing the details of a Holding - i.e. a batch"""
    __tablename__ = "holding"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the Integer representation of the UUID, for
    # speed in searching and comparing
    transaction_id = Column(Integer, nullable=False, index=True)
    # list of free-text tags
    tags = Column(String)


class File(Base):
    """Class containing the details of a single File"""
    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # transaction id, Integer as above
    transaction_id = Column(Integer, nullable=False, index=True)
    # holding id as ForeignKey
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)
    holding = relationship("Holding", back_populates="holding")
    # original path on POSIX disk
    original_path = Column(String)
    # filetype, one of:
    #   FILE,
    #   DIRectory, 
    #   LCOM: link common path, 
    #   LABS: link absolute path
    filetype = Column(String(4))
    # path to the link
    link_path = Column(String)
    # file size, in kilobytes
    size = Column(Integer)
    # user name file belongs to
    user = Column(Integer)
    # user group file belongs to
    group = Column(String)
    # unix style file permissions
    file_permissions = Column(Integer)
    # last time the file was accessed
    access_time = Column(DateTime)
    # name of the object on the object store
    object_name = Column(String)
