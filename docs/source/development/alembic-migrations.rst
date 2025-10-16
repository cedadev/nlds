Database Migrations with Alembic
================================

Alembic is the migration tool that comes packaged with SQLAlchemy. 


Using Alembic Migrations
------------------------

From an end-user stand point, using alembic is relatively straight forward. If 
you're using your local development environment or deploying to kubernetes (or 
wherever) the migrations should be designed to be applied in the same manner. 
Basically, the steps consist of:

1. Make sure alembic is installed (the correct version should be in the 
``requirements.txt``)
2. Change directory to the root of the repo
3. Run ``alembic upgrade head``, which should run all of the pending migrations 
in the correct order and update your database to the latest version. 

Writing Alembic Migrations
--------------------------

Writing alembic migrations is a little more complicated unfortunately, because 
we have to account for both SQLite local databases and the PostgreSQL production 
database, as well as migrating two databases (the catalog and the monitoring db)
at the same time. Fortunately it's not super complex, and can be done `mostly` 
automatically. `Mostly`...

So in general we can summarise the steps for a generic upgrade procedure as 
follows:

1. Do schema migration on database (e.g. add table, add columns, change defaults)
2. Do data migration (e.g. change values in a column or table)
3. Do rest of schema migration (e.g. remove columns, add or remove foreign key 
constraints)

Alembic has auto-generation functionality, which can be useful to generate a 
rough outline of the schema migration you want to apply. This can be generated 
by using the command::

   alembic revision --autogenerate -m '{DESCRIPTION}'

where ``{DESCRIPTION}`` should be replaced with a short description of what the 
migration is changing in the database. Current convention is to use all lower 
case. 

This should provide a filled in template which gets you a good deal of the way 
there in terms of schema migration. This will need to be altered so that 
``batch_update_table`` is used in lieu of the regular ``update_table`` which 
doesn't work with SQLite (basically implements table altering by deleting and 
remaking a given table). For examples of this see past migrations and for more 
details read the `SQLAlchemy documentation <https://alembic.sqlalchemy.org/en/latest/batch.html>`_
on it. 

For the data migration we can use the standard ORM model that's in use across 
the rest of the NLDS database interaction, but in order to do this safely and 
repeatably we have to make a local copy, in the revision file, of the database 
model. This is good practice so as to ensure the migration isn't broken by 
imports that change in future database migrations. To do it yourself, you 
basically need to copy the class Contents of either ``catalog_models.py`` or 
``monitor_models.py`` into the revision file, including both old and new changes 
to the table fields. You can then make a ``Session`` and query/update the 
database as you normally would. Again, look at previous revisions for examples 
of this, in particular ``ee82dd99bfc0_add_storage_url_fields.py``.

TODO: Flesh this out, edit for clarity. 