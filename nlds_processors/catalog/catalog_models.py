from __future__ import annotations

# encoding: utf-8
"""
catalog_models.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

"""Declare the SQLAlchemy ORM models for the NLDS Catalog database"""

import enum
from urllib.parse import urlunsplit

from sqlalchemy import (
    Integer,
    String,
    Column,
    DateTime,
    Enum,
    BigInteger,
    UniqueConstraint,
    Boolean,
)

from sqlalchemy import ForeignKey
from sqlalchemy.orm import declarative_base, relationship


from nlds.details import PathType
from nlds_processors.catalog.catalog_error import CatalogError
import nlds.rabbit.message_keys as MSG

"""Declarative base class, containing the Metadata object"""
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
    __table_args__ = (UniqueConstraint("label", "user"),)

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

    # get the prefix
    def get_prefix(self):
        """Gets a unique prefix from the holding that will be used as the directory on
        tape"""
        prefix = f"{self.id}.{self.user}.{self.group}"
        return prefix


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
    holding_id = Column(Integer, ForeignKey("holding.id"), index=True, nullable=False)


class Tag(CatalogBase):
    """Class containing the details of a Tag that can be assigned to a Holding"""

    __tablename__ = "tag"
    # primary key
    id = Column(Integer, primary_key=True)
    # key:value tags - key should be unique per holding_id
    key = Column(String)
    value = Column(String)
    # holding id as ForeignKey "Parent"
    holding_id = Column(Integer, ForeignKey("holding.id"), index=True, nullable=False)

    __table_args__ = (UniqueConstraint("key", "holding_id"),)


class File(CatalogBase):
    """Class containing the details of a single File"""

    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # transaction id as ForeignKey "Parent"
    transaction_id = Column(
        Integer, ForeignKey("transaction.id"), index=True, nullable=False
    )
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
    locations = relationship("Location", cascade="delete, delete-orphan")
    # relationship for checksum (one to one)
    checksums = relationship("Checksum", cascade="delete, delete-orphan")


class Storage(enum.Enum):
    OBJECT_STORAGE = 1
    TAPE = 2

    def to_json(self):
        return [MSG.OBJECT_STORAGE, MSG.TAPE][self.value - 1]

    @classmethod
    def from_str(cls, storage_type: str):
        if storage_type == MSG.OBJECT_STORAGE:
            return cls(cls.OBJECT_STORAGE)
        elif storage_type == MSG.TAPE:
            return cls(cls.TAPE)
        else:
            raise CatalogError(f"{storage_type}: unknown Storage_Type in Storage")

    def __str__(self):
        return [MSG.OBJECT_STORAGE, MSG.TAPE][self.value - 1]


class Location(CatalogBase):
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
    file_id = Column(Integer, ForeignKey("file.id"), index=True, nullable=False)
    aggregation_id = Column(
        Integer, ForeignKey("aggregation.id"), index=True, nullable=True
    )

    # storage_type must be unique per file_id, i.e. each file can only have one
    # each location
    __table_args__ = (UniqueConstraint("storage_type", "file_id"),)

    @property
    def url(self) -> str | None:
        """Get the 1st object storage location and return the url
        url = f"{}
        """
        if self.storage_type == Storage.OBJECT_STORAGE:
            # only object storage returns a URL
            return urlunsplit(
                (
                    self.url_scheme,
                    self.url_netloc,
                    f"nlds.{self.root}/{self.path}",
                    "",
                    "",
                )
            )
        else:
            return ""


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
    file_id = Column(Integer, ForeignKey("file.id"), index=True, nullable=False)
    # checksum must be unique per algorithm
    __table_args__ = (UniqueConstraint("checksum", "algorithm"),)


class Aggregation(CatalogBase):
    """Class containing the details of file aggregations made for writing files
    to tape (specifically CTA) as tars"""

    __tablename__ = "aggregation"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # The name of the tarfile on tape
    tarname = Column(String, nullable=False)
    # checksum
    checksum = Column(String, nullable=True)
    # checksum method / algorithm
    algorithm = Column(String, nullable=True)
    # whether aggregation has failed or not
    failed_fl = Column(Boolean, nullable=False)

    # relationship for location (one to many)
    locations = relationship("Location", cascade="delete, delete-orphan")

class Quota(CatalogBase):
    """Class containing the details of quota values for a Group."""

    __tablename__ = "quota"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # user group quota belongs to 
    group = Column(String)
    # size of the quota for the group
    size = Column(Integer)
    # amount of quota used 
    used = Column(Integer)

    def to_dict(self):
        return {
            "id": self.id,
            "group": self.group,
            "size": self.size,
            "used": self.used,
        }