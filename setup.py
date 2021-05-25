from setuptools import setup, find_packages
from os import path


with open('requirements.txt') as f:
    required = f.read().splitlines()

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version='1.0.3',
    name='elastic-tools',
    install_requires=required,
    packages=find_packages(),
    url='https://github.com/petricore-tech/elastictools.git',
    entry_points={
        'console_scripts': [
            'mongo2elastic=mongo.mongo2elastic:run',
            'elastic2mongo=mongo.elastic2mongo:run',
            'elasticdump=backup.dump:run',
            'elasticload=backup.load:run'
        ]
    },
    long_description=long_description,
    long_description_content_type='text/markdown'
)