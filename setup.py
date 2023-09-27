from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='etl_kommatipara',
    packages=find_packages(exclude=['tests']),
    install_requires=requirements
)
