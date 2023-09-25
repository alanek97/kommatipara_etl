
from setuptools import setup, find_packages

setup(
    name='etl_kommatipara',
    packages=find_packages(where='src',include=['utils', 'etl_source_code']),
    package_dir={"": "src"}
)