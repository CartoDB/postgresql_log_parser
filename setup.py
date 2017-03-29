"""
CARTO postgresql log parser
"""

from setuptools import setup, find_packages

setup(
    name="postgresql_log_parser",
    version='0.0.1',
    description='A postgresql log parser',
    url='https://github.com/cartodb/postgresql_log_parser',
    author='CARTO Engine Team',
    author_email='engine@carto.com',
    license='MIT',
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Intended Audience :: ',
        'Topic :: ',
        'License :: OSI Approbed :: MIT License',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='postgresql log parser',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['pyparsing'],
    extras_require={
        'dev': ['ipdb', 'ipython'],
        'test': ['nose']
    }
)
