"""Declare the SQLAlchemy ORM models for the NLDS Catalog database"""
from __future__ import annotations

from sqlalchemy import Integer, String, Column, DateTime, Enum, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, relationship

import enum
from nlds.details import PathType

"""Declarative base class, containing the Metadata object"""
Base = declarative_base()

class Holding(Base):
    """Class containing the details of a Holding - i.e. a batch"""
    __tablename__ = "holding"
    # primary key / integer id / batch id
    id = Column(Integer, primary_key=True)
    # transaction id - this will be the String
    transaction_id = Column(String, nullable=False, index=True)
    # relationship for tags (One to many)
    tags = relationship("Tag", backref="holding", cascade="delete")
    # relationship for files (One to many)
    files = relationship("File", backref="holding", cascade="delete")
    # date and time of ingest / adding to catalogue
    ingest_time = Column(DateTime)


class Tag(Base):
    """Class containing the details of a Tag that can be assigned to a Holding"""
    __tablename__ = "tag"
    # primary key
    id = Column(Integer, primary_key=True)
    # free text tag
    tag = Column(String)
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)

class File(Base):
    """Class containing the details of a single File"""
    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), 
                        index=True, nullable=False)
    # original path on POSIX disk
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
    location = relationship("Location", backref="file")
    # relationship for checksum (one to one)
    checksum = relationship("Checksum", backref="file")


class Storage(enum.Enum):
    OBJECT_STORAGE = 1
    TAPE = 2


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

class Checksum(Base):
    """Class containing checksum and algorithm used to calculate checksum"""
    __tablename__ = "checksum"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # checksum
    checksum = Column(String, nullable=False)
    # checksum method / algorithm
    algorithm = Column(String, nullable=False)
    # file id as ForeignKey "Parent" (one to one)
    file_id = Column(Integer, ForeignKey("file.id"), 
                     index=True, nullable=False)