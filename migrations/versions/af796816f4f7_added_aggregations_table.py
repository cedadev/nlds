"""Added aggregations table

Revision ID: af796816f4f7
Revises: 991b6f8cf6be
Create Date: 2023-07-05 17:37:13.373813

"""
from uuid import uuid4

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Integer, String, Column, DateTime, Enum, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, Session

from nlds_processors.catalog.catalog_models import CatalogBase
from nlds.details import PathType


# revision identifiers, used by Alembic.
revision = 'af796816f4f7'
down_revision = '991b6f8cf6be'
branch_labels = None
depends_on = None


# recreating table declarations here so this revision isn't broken by importing 
# future updates to these tables. 
class Transaction(CatalogBase):
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

class Aggregation(CatalogBase):
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

class File(CatalogBase):
    """Class containing the details of a single File"""
    __tablename__ = "file"
    # primary key / integer id
    id = Column(Integer, primary_key=True)
    # aggregation_id as ForeignKey "Parent"
    aggregation_id = Column(Integer, ForeignKey("aggregation.id"), 
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


def upgrade(engine_name: str) -> None:
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name: str) -> None:
    globals()["downgrade_%s" % engine_name]()



def upgrade_catalog() -> None:
    # ### commands auto generated by Alembic - please adjust! ###

    # Schema migration - add aggregation table and aggregation_id table
    op.create_table('aggregation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('aggregation_id', sa.String(), nullable=False),
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['transaction_id'], ['transaction.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.add_column('file', sa.Column('aggregation_id', sa.Integer(), nullable=False))

    # Create some indices
    op.create_index(op.f('ix_aggregation_aggregation_id'), 'aggregation', 
                    ['aggregation_id'], unique=False)
    op.create_index(op.f('ix_aggregation_transaction_id'), 'aggregation', 
                    ['transaction_id'], unique=False)

    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())
    
    # Make a 1-1 correspondence of existing transactions with new aggregations
    transactions = session.query(Transaction).all()
    for t in transactions:
        agg = Aggregation(
            id=t.id,
            aggregation_id=uuid4,
            transaction_id=t.id,
        )
        session.add(agg)
        
    # Then just update each of the Files to point to the new aggregation instead 
    # of the transaction. The ids are the same so it should be a simple 
    # switchover...
    files = session.query(File).all()
    for f in files:
        f.aggregation_id = f.transaction_id
    session.commit()

    # Now we can get on with the rest of the migration
    op.drop_index('ix_file_transaction_id', table_name='file')
    op.create_index(op.f('ix_file_aggregation_id'), 'file', ['aggregation_id'], 
                    unique=False)
    op.drop_constraint(None, 'file', type_='foreignkey')
    op.create_foreign_key(None, 'file', 'aggregation', ['aggregation_id'], 
                          ['id'])
    op.drop_column('file', 'transaction_id')


def downgrade_catalog() -> None:
    # Do bare minimum of schema migration to then apply data migration
    op.add_column('file', sa.Column('transaction_id', sa.INTEGER(), 
                                    nullable=False))
    
    # Make a session to iterate over the data and make necessary changes. 
    session = Session(bind=op.get_bind())

    # Switch files to point to the transaction of the aggregation they're 
    # pointing to
    files = session.query(File).all()
    for f in files:
        agg = session.query(Aggregation).filter(
            f.aggregation_id == Aggregation.id
        ).one()
        f.transaction_id = agg.transaction_id

    # Carry on with the rest of the schema migration
    op.drop_constraint(None, 'file', type_='foreignkey')
    op.create_foreign_key(None, 'file', 'transaction', ['transaction_id'], ['id'])
    op.drop_index(op.f('ix_file_aggregation_id'), table_name='file')
    op.create_index('ix_file_transaction_id', 'file', ['transaction_id'], unique=False)
    op.drop_column('file', 'aggregation_id')
    op.drop_index(op.f('ix_aggregation_transaction_id'), table_name='aggregation')
    op.drop_index(op.f('ix_aggregation_aggregation_id'), table_name='aggregation')
    op.drop_table('aggregation')
    # ### end Alembic commands ###


def upgrade_monitor() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_monitor() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

