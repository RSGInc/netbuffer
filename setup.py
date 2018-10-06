from ez_setup import use_setuptools
from setuptools import setup, find_packages
use_setuptools()  # nopycodestyle

with open('README.rst') as file:
    long_description = file.read()

setup(
    name='netbuffer',
    version='0.3',
    description='Network based queries and aggregations on land use data',
    author='contributing authors',
    author_email='scoe@psrc.org',
    license='BSD-3',
    url='https://github.com/psrc/netbuffer',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: BSD License'
    ],
    long_description=long_description,
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'numpy >= 1.8.0',
        'pandas >= 0.18.0',
        'orca >= 1.1',
        'activitysim >= 0.6',
        'pandana == 0.3'
    ]
)
