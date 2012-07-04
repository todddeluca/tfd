

'''
GO as in Gene Ontology.

ftp://ftp.geneontology.org/pub/go/godatabase/archive/README

Example usage:

download()
process()
import pandas
print pandas.DataFrame(list(term_table_dicts(".", guess_latest_release())))[:5]


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
    '''
    if today is None:
        today = datetime.date.today()
    if today.month == 1:
        release = datetime.date(today.year - 1, 12, 1)
    else:
        release = datetime.date(today.year, today.month - 1, 1)
    return release.isoformat()


def term_tables_basename(release):
    '''
    e.g. 'go_201206-termdb-tables.tar.gz'
    '''
    year, month, day = release.split('-')
    ym_release = year + month
    return 'go_{}-termdb-tables.tar.gz'.format(ym_release)


def term_tables_url(release):
    '''
    e.g. 'ftp://ftp.geneontology.org/pub/go/godatabase/archive/full/2012-06-01/go_201206-termdb-tables.tar.gz'
    '''
    basename = term_tables_basename(release)
    url_root = 'ftp://ftp.geneontology.org/pub/go/godatabase/archive'
    return url_root + '/' + 'full/{}/{}'.format(release, basename)


def term_tables_file(root, release):
    '''
    e.g. './data/geneontology/go_201206-termdb-tables.tar.gz'
    '''
    basename = term_tables_basename(release)
    return os.path.join(root, 'geneontology', basename)


def term_tables_dir(root, release):
    '''
    e.g. './data/geneontology/go_201206-termdb-tables'
    '''
    tarfile = term_tables_file(root, release)
    assert tarfile.endswith('.tar.gz')
    return tarfile[:-7] # remove '.tar.gz'


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


def process(root='.', release=None):
    release = release if release else guess_latest_release()
    path = term_tables_file(root, release)
    print 'processing', path
    if path.endswith('.tar.gz'):
        print '...tar xzf file'
        subprocess.check_call(['tar', '-xzf', os.path.basename(path)],
                              cwd=os.path.dirname(path))


def term_table_fields():
    '''
    From the CREATE TABLE statement in the term.sql file we can see that the
    fields are:
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `name` varchar(255) NOT NULL DEFAULT '',
        `term_type` varchar(55) NOT NULL,
        `acc` varchar(255) NOT NULL,
        `is_obsolete` int(11) NOT NULL DEFAULT '0',
        `is_root` int(11) NOT NULL DEFAULT '0',
        `is_relation` int(11) NOT NULL DEFAULT '0',

    Return a tuple containing all the field names.
    '''
    return ('id', 'name', 'term_type', 'acc', 'is_obsolete', 'is_root',
            'is_relation')


def term_table_tuples(root, release):
    '''
    From the CREATE TABLE statement in the term.sql file we can see that the
    fields are:
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `name` varchar(255) NOT NULL DEFAULT '',
        `term_type` varchar(55) NOT NULL,
        `acc` varchar(255) NOT NULL,
        `is_obsolete` int(11) NOT NULL DEFAULT '0',
        `is_root` int(11) NOT NULL DEFAULT '0',
        `is_relation` int(11) NOT NULL DEFAULT '0',

    Iterate over the rows of the table term.txt, yielding a tuple of the
    row values converted to int or str as appropriate.
    '''
    field_types = (int, str, str, str, int, int, int)
    num = len(field_types)
    with open(term_table_file(root, release)) as fh:
        for line in fh:
            fields = line.strip().split('\t')
            yield tuple(field_types[i](fields[i]) for i in range(num))


def term_table_dicts(root, release):
    '''
    From the CREATE TABLE statement in the term.sql file we can see that the
    fields are:
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `name` varchar(255) NOT NULL DEFAULT '',
        `term_type` varchar(55) NOT NULL,
        `acc` varchar(255) NOT NULL,
        `is_obsolete` int(11) NOT NULL DEFAULT '0',
        `is_root` int(11) NOT NULL DEFAULT '0',
        `is_relation` int(11) NOT NULL DEFAULT '0',

    Return a dict for each row in the term.txt file mapping the field
    name to the field value.  Field values will be converted to int or str as
    appropriate.
    '''
    field_names = term_table_fields()
    num = len(field_names)
    for fields in term_table_tuples(root, release):
        yield {field_names[i]: fields[i] for i in range(num)}



