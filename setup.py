from setuptools import setup, find_packages


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    version='1.0',
    name='elastictools',
    install_requires=required,
    packages=find_packages(),
    entry_points={'console_scripts': ['mongo2elastic=mongo.mongo2elastic:run']}
)