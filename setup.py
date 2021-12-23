import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='nlds',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'fastapi',
        'uvicorn',
        'requests',
        'retry',
        'pika'
    ],
    include_package_data=True,
    package_data={
        'nlds': ['templates/*.j2'],
        'nlds_processors': ['templates/*.j2']
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
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)
