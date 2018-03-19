from ez_setup import use_setuptools
use_setuptools()  # nopep8

from setuptools import setup, find_packages

with open('README.rst') as file:
    long_description = file.read()

setup(
    name='netbuffer',
    version='0.1',
    description='Network based queries and aggregations on land use data',
    author='contributing authors',
    author_email='ben.stabler@rsginc.com',
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
        'activitysim >= 0.5',
        'pandana == 0.3'
    ]
)
