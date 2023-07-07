"""add aggregations table

Revision ID: af796816f4f7
Revises: 991b6f8cf6be
Create Date: 2023-07-05 17:37:13.373813

"""
from uuid import uuid4

from alembic import op
import sqlalchemy as sa
from sqlalchemy import (Integer, String, Column, DateTime, Enum, BigInteger, 
                        UniqueConstraint)
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, Session, declarative_base
from sqlalchemy.sql.expression import text

from nlds_processors.catalog.catalog_models import Storage
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = 'af796816f4f7'
down_revision = '991b6f8cf6be'
branch_labels = None
depends_on = None

Base = declarative_base()


# recreating table declarations here so this revision isn't broken by importing 
# future updates to these tables. 
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



def upgrade_catalog() -> None:
    # Schema migration - add aggregation table and aggregation_id table
    op.create_table('aggregation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('aggregation_id', sa.String(), nullable=False),
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['transaction_id'], ['transaction.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    # Initially add the column as nullable, then edit after we've populated it
    op.add_column('file', sa.Column('aggregation_id', sa.Integer(), 
                                    nullable=False, server_default=text("0")))

    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())
    
    # Make a 1-1 correspondence of existing transactions with new aggregations
    transactions = session.query(Transaction).all()
    for t in transactions:
        agg = Aggregation(
            id=t.id,
            aggregation_id=str(uuid4()), # Create a new uuid, only option really 
            transaction_id=t.id,
        )
        session.add(agg)
        
    # Then just update each of the Files to point to the new aggregation instead 
    # of the transaction. The ids are the same so it should be a simple 
    # switchover...
    files = session.query(File).all()
    for f in files:
        f.aggregation_id = f.transaction_id
    # Commit the session to update the db
    session.commit()

    # Now we can get on with the rest of the schema migration
    # Create the new indices and remove the old
    op.create_index(op.f('ix_aggregation_aggregation_id'), 'aggregation', 
                    ['aggregation_id'], unique=False)
    op.create_index(op.f('ix_aggregation_transaction_id'), 'aggregation', 
                    ['transaction_id'], unique=False)
    op.create_index(op.f('ix_file_aggregation_id'), 'file', 
                    ['aggregation_id'], unique=False)
    op.drop_index('ix_file_transaction_id', table_name='file')
    # We wrap it in a batch context manager so SQLite can be migrated/altered 
    # too (see https://alembic.sqlalchemy.org/en/latest/batch.html for details)
    # Here we also specify a naming convention so that the by-default-unnamed 
    # foreign key constraints can be added/removed. This will need to be copied 
    # to all future revisions.
    naming_conv = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    }
    with op.batch_alter_table("file", naming_convention=naming_conv) as bop:
        # Remove the server_default from the agg_id column. 
        bop.alter_column("aggregation_id", existing_server_default=None, 
                         existing_nullable=False, existing_type=sa.Integer())
        # Drop the fk constraint and remake it (don't need to drop it as we're 
        # dropping the column at the end anyway)
        # bop.drop_constraint("fk_files_transaction_ids_transactions", 
        #                     type_='foreignkey')
        bop.create_foreign_key("fk_files_aggregation_ids_aggregations", 
                               'aggregation', ['aggregation_id'], ['id'])
        # Finally get rid of the transaction_id column
        bop.drop_column('transaction_id')


def downgrade_catalog() -> None:
    # Do bare minimum of schema migration to then apply data migration
    op.add_column('file', sa.Column('transaction_id', sa.INTEGER(), 
                                    nullable=False, server_default=text("0")))
    
    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())

    # Switch files to point to the transaction of the aggregation they're 
    # pointing to
    aggs = session.query(Aggregation).all()
    for agg in aggs:
        files = session.query(File).filter(File.aggregation_id == agg.id).all()
        for f in files:
            f.transaction_id = agg.transaction_id
    # Commit the changes
    session.commit()

    # Carry on with the rest of the schema migration
    naming_conv = {
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    }
    with op.batch_alter_table("file", naming_convention=naming_conv) as bop:
        # Remove the server_default from the agg_id column. 
        bop.alter_column("transaction_id", existing_server_default=None, 
                         existing_nullable=False, existing_type=sa.Integer())
        # Create a new foreign key constraint according to naming convention
        bop.create_foreign_key("fk_files_transaction_ids_transactions", 
                              'transaction', ['transaction_id'], ['id'])
        bop.drop_column('aggregation_id')
    
    # Sort out indices and, finally, drop the aggregations table
    op.create_index('ix_file_transaction_id', 'file', ['transaction_id'], 
                    unique=False)
    op.drop_index(op.f('ix_file_aggregation_id'), table_name='file')
    op.drop_index(op.f('ix_aggregation_transaction_id'), 
                  table_name='aggregation')
    op.drop_index(op.f('ix_aggregation_aggregation_id'), 
                  table_name='aggregation')
    op.drop_table('aggregation')


def upgrade_monitor() -> None:
    # No changes to montioring db
    pass


def downgrade_monitor() -> None:
    # No changes to montioring db
    pass

