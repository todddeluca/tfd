

'''
GO as in Gene Ontology.

ftp://ftp.geneontology.org/pub/go/godatabase/archive/README

Example usage:

import pandas
import tfd.go
install_dir = tfd.go.install_in_dir()
go = tfd.go.GeneOntology(install_dir)
print pandas.DataFrame(list(go.term_table_dicts()))[:5]

'''

import datetime
import os
import subprocess


DIR_MODE = 0775 # directories in this dataset are world readable and group writable.

def guess_latest_release(today=None):
    '''
    Full releases seem to be published sometime in the first or second week
    of the month and named using the first day of the month, so a conservative
    guess which will get the lastest or penultimate release would be the first
    day of the previous month.  
    
    Return the release as an iso format string, YYYY-MM-DD.  E.g. '2012-06-01'.

    today: Used for testing that the return value really is the first of the
    previous month, even when "today" is in January.
    '''
    if today is None:
        today = datetime.date.today()
    if today.month == 1:
        release = datetime.date(today.year - 1, 12, 1)
    else:
        release = datetime.date(today.year, today.month - 1, 1)
    return release.isoformat()


def install_in_dir(root=None, release=None):
    '''
    Download and unpack a Gene Ontology termdb release under a local dir.

    root: a directory in which to install the termdb tables release.  Defaults
    to the current directory.
    release: the specific release to download and install, as a iso formatted
    date string.  E.g. '2012-06-01'.  Defaults to one of the more recent
    releases.

    return: the directory in which the release was installed.  This should be
    a dir under root named after the release, e.g.
    '/path/to/root/go_201206-termdb-tables'
    This dir is useful for creating a GeneOntology object.
    '''
    root = os.path.abspath(root) if root is not None else os.getcwd()
    release = release or guess_latest_release()
    url = term_tables_url(release)
    destbase = term_tables_basename(release)
    dest = os.path.join(root, destbase) # download to here

    # download to /path/to/root/go_201206-termdb-tables.tar.gz
    if not os.path.exists(root):
        os.makedirs(root, DIR_MODE)
    print 'downloading {} to {}...'.format(url, dest)
    cmd = ['curl', '--remote-time', '--output', dest, url]
    subprocess.check_call(cmd)

    # untar to /path/to/root/go_201206-termdb-tables
    print 'processing tarball', dest
    print '...tar xzf file'
    subprocess.check_call(['tar', '-xzf', destbase],
                            cwd=root)

    print 'removing tarball', dest
    os.remove(dest)

    # return /path/to/root/go_201206-termdb-tables
    return term_tables_dir(root, release)


def term_tables_dir(root=None, release=None):
    '''
    Return the directory in which the release should be installed under root.
    This is useful for getting the dir for creating a GeneOntology object.
    e.g. '/path/to/root/go_201206-termdb-tables'
    '''
    root = os.path.abspath(root) if root is not None else os.getcwd()
    release = release or guess_latest_release()
    tarfile = term_tables_file(root, release)
    return tarfile[:-7] # remove '.tar.gz'


def term_tables_basename(release):
    '''
    e.g. 'go_201206-termdb-tables.tar.gz'
    '''
    year, month, day = release.split('-')
    basename = 'go_{}-termdb-tables.tar.gz'.format(year + month)
    assert basename.endswith('.tar.gz')
    return basename


def term_tables_url(release):
    '''
    e.g. 'ftp://ftp.geneontology.org/pub/go/godatabase/archive/full/2012-06-01/go_201206-termdb-tables.tar.gz'
    '''
    basename = term_tables_basename(release)
    url_root = 'ftp://ftp.geneontology.org/pub/go/godatabase/archive'
    return url_root + '/' + 'full/{}/{}'.format(release, basename)


def term_tables_file(root, release):
    '''
    e.g. '/path/to/root/go_201206-termdb-tables.tar.gz'
    '''
    basename = term_tables_basename(release)
    return os.path.join(root, basename)


class GeneOntology(object):
    '''
    An abstraction layer around a filesystem installation of a Gene
    Ontology release.  This object provides access to the tables in 
    Gene Ontology.  Well, currently only the term table.

    From the CREATE TABLE statement in the term.sql file we can see that
    the fields are:
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `name` varchar(255) NOT NULL DEFAULT '',
        `term_type` varchar(55) NOT NULL,
        `acc` varchar(255) NOT NULL,
        `is_obsolete` int(11) NOT NULL DEFAULT '0',
        `is_root` int(11) NOT NULL DEFAULT '0',
        `is_relation` int(11) NOT NULL DEFAULT '0',
    '''
    def __init__(self, release_dir):
        '''
        release_dir:  The directory the release is installed in, e.g.
        '/path/to/root/go_201206-termdb-tables'
        '''
        self.release_dir = release_dir

    def _term_table_file(self):
        '''
        Return the path to the term.txt file of the release.
        e.g. '/path/to/root/go_201206-termdb-tables/term.txt'
        '''
        return os.path.join(self.release_dir, 'term.txt')

    def term_table_fields(self):
        '''
        Return a tuple containing all the field names.
        '''
        return ('id', 'name', 'term_type', 'acc', 'is_obsolete', 'is_root',
                'is_relation')

    def term_table_tuples(self):
        '''
        Iterate over the rows of the term table, yielding a tuple of the
        row values converted to int or str as appropriate.
        '''
        field_types = (int, str, str, str, int, int, int)
        num = len(field_types)
        with open(self._term_table_file()) as fh:
            for line in fh:
                fields = line.strip().split('\t')
                yield tuple(field_types[i](fields[i]) for i in range(num))

    def term_table_dicts(self):
        '''
        Iterate over the rows of the term table, yielding a dict mapping 
        each field name to the field value for the row, converted to int or
        str as appropriate.
        '''
        field_names = self.term_table_fields()
        num = len(field_names)
        for fields in self.term_table_tuples():
            yield {field_names[i]: fields[i] for i in range(num)}


############
# Deprecated


def term_table_file(root, release):
    '''
    Return the path to the term.txt file of the release.
    e.g. './data/geneontology/go_201206-termdb-tables/term.txt'
    '''
    return os.path.join(term_tables_dir(root, release), 'term.txt')


def download(root='.', release=None):
    '''
    Download go termdb tables.

    root: dir under which to save downloaded files.
    release: if None, the lastest release will be downloaded.
    '''
    release = release if release else guess_latest_release()
    url = term_tables_url(release)
    dest = term_tables_file(root, release)

    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest), DIR_MODE)
    print 'downloading {} to {}...'.format(url, dest)
    cmd = ['curl', '--remote-time', '--output', dest, url]
    subprocess.check_call(cmd)
    return dest


def process(root='.', release=None):
    release = release if release else guess_latest_release()
    path = term_tables_file(root, release)
    print 'processing', path
    if path.endswith('.tar.gz'):
        print '...tar xzf file'
        subprocess.check_call(['tar', '-xzf', os.path.basename(path)],
                              cwd=os.path.dirname(path))



