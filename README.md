
## Introduction

`tfd` is a python package containing a variety of modules that do not seem to
warrant their own distribution or package.  It provides a convenient namespace
to avoid conflicts with other modules and packages out there.

This package is _alpha_ and its API is not stable.


## Contribute

Feel free to make a pull request on github.


## Requirements

- Probably Python 2.7 (since that is the only version it has been tested with.)


## Installation

### Install from pypi.python.org

Download and install using pip:

    pip install tfd

### Install from github.com

Using github, one can clone and install a specific version of the package:

    cd ~
    git clone git@github.com:todddeluca/tfd.git
    cd tfd
    python setup.py install


## Usage

An example of usage from within a python module:

    import tfd.go
    tfd.go.download()

Please see the individual modules for more uses.




