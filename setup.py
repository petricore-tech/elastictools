from setuptools import setup, find_packages


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    version='1.0',
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
    }
)