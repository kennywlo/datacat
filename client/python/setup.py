from setuptools import setup

requires = [
    'requests'
]

setup(
    name='datacat',
    version='0.6.1',
    packages=['datacat'],
    url='https://supercdms-dev.slac.stanford.edu',
    license='SLAC BSD',
    author='Kenny Lo',
    author_email='kennywlo@slac.stanford.edu',
    description='Datacat client library',
    install_requires=requires,
    entry_points={
        'console_scripts': ['datacat = datacat.cli:main'],
    }
)
