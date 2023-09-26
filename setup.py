from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='etl_kommatipara',
    packages=find_packages(where='src',include=['utils', 'etl_source_code']),
    package_dir={"": "src"},
    install_requires=requirements
)