# encoding: utf-8
"""
setup.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import os
from setuptools import setup, find_packages
from nlds import __version__

with open(os.path.join(os.path.dirname(__file__), "README.md")) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name="nlds",
    version=__version__,
    packages=find_packages(),
    install_requires=[
        "alembic",
        "anyio",
        "asgiref",
        "certifi",
        "charset-normalizer",
        "click",
        "cryptography",
        "decorator",
        "fastapi",
        "h11",
        "idna",
        "Jinja2",
        "Mako",
        "MarkupSafe",
        "minio",
        "pika",
        "py",
        "pydantic",
        "requests",
        "retry",
        "sniffio",
        "SQLAlchemy",
        "starlette",
        "typing_extensions",
        "urllib3",
        "uvicorn",
    ],
    include_package_data=True,
    package_data={
        "nlds": ["templates/*"],
        "nlds_processors": ["templates/*.j2"],
        "scripts": ["*"],
    },
    license="LICENSE.txt",  # example license
    description=("REST-API server and consumers for CEDA Near-Line Data Store"),
    long_description=README,
    url="http://www.ceda.ac.uk/",
    author="Neil Massey & Jack Leland",
    author_email="neil.massey@stfc.ac.uk",
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: RestAPI",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    entry_points={
        "console_scripts": [
            "nlds_q=nlds_processors.nlds_worker:main",
            "catalog_q=nlds_processors.catalog.catalog_worker:main",
            "index_q=nlds_processors.index:main",
            "monitor_q=nlds_processors.monitor.monitor_worker:main",
            "transfer_put_q=nlds_processors.transfer.put_transfer:main",
            "transfer_get_q=nlds_processors.transfer.get_transfer:main",
            "logging_q=nlds_processors.logger:main",
            "archive_put_q=nlds_processors.archive.archive_put:main",
            "archive_get_q=nlds_processors.archive.archive_get:main",
            "send_archive_next=nlds_processors.archive.send_archive_next:send_archive_next",
        ],
    },
)
