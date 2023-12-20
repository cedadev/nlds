import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='nlds',
    version='0.0.2',
    packages=find_packages(),
    install_requires=[
        'fastapi',
        'uvicorn',
        'requests',
        'retry',
        'pika',
        'minio',
        'sqlalchemy'
    ],
    include_package_data=True,
    package_data={
        'nlds': ['templates/*'],
        'nlds_processors': ['templates/*.j2'],
        'scripts': ['*'],
    },
    license='LICENSE.txt',  # example license
    description=('REST-API server for CEDA Near-Line Data Store'),
    long_description=README,
    url='http://www.ceda.ac.uk/',
    author='Neil Massey',
    author_email='neil.massey@stfc.ac.uk',
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: RestAPI',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
    entry_points = {
        'console_scripts': [
            'nlds_q=nlds_processors.nlds_worker:main',
            'catalog_q=nlds_processors.catalog.catalog_worker:main',
            'index_q=nlds_processors.index:main',
            'monitor_q=nlds_processors.monitor.monitor_worker:main',
            'transfer_put_q=nlds_processors.transferers.put_transfer:main',
            'transfer_get_q=nlds_processors.transferers.get_transfer:main',
            'transfer_del_q=nlds_processors.transferers.del_transfer:main',
            'logging_q=nlds_processors.logger:main',
            'archive_put_q=nlds_processors.archiver.archive_put:main',
            'archive_get_q=nlds_processors.archiver.archive_get:main',
            'archive_del_q=nlds_processors.archiver.archive_del:main',
            'send_archive_next=nlds_processors.archiver.send_archive_next:send_archive_next',
        ],
    }
)
