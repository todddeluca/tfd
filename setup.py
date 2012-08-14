
import os
from setuptools import setup, find_packages

setup(
    name = 'tfd',
    version = '0.2.2',
    license = 'MIT',
    description = 'Random modules in a personal namespace.',
    long_description = open(os.path.join(os.path.dirname(__file__), 
                                         'README.md')).read(),
    keywords = 'python gene ontology',
    url = 'https://github.com/todddeluca/tfd',
    author = 'Todd Francis DeLuca',
    author_email = 'todddeluca@yahoo.com',
    classifiers = ['License :: OSI Approved :: MIT License',
                   'Development Status :: 3 - Alpha',
                   'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 2.7',
                  ],
    packages = ['tfd'],
)

