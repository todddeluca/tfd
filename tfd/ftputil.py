

import contextlib
import ftplib
import time
import urlparse


@contextlib.contextmanager
def connect_and_login(url, ftp=None, username=None, password=None):
    '''
    Connect to and login to an ftp server and yield the FTP object.  The
    ftp connection will be closed after the yield statement.

    :param ftp: If not None, simply yield ftp; no connecting, logging in, or
    closing of ftp connections will happen.  Passing through an ftp object
    in this manner can be useful for reusing and existing ftp connection.
    '''

    if ftp is not None:
        yield ftp
    else:
        # extract host and port
        result = urlparse.urlsplit(url)
        host = result.hostname
        port = result.port
        # connect
        ftp = ftplib.FTP()
        if port is None:
            ftp.connect(host)
        else:
            ftp.connect(host, port)

        try:
            if username is None:
                ftp.login()
            elif password is None:
                ftp.login(username)
            else:
                ftp.login(username, password)
            yield ftp
        except:
            ftp.quit()
            raise



def walk(url, depth=-1, conn=None, pause=0, username=None, password=None):
    '''
    Traverse a directory tree rooted at url.  Similar to os.walk.  Walks the
    directories (in a depth-first manner) starting at url, yielding tuples of
    (currentUrl, dirnames, filenames), where currentUrl is url or a subdir of
    url, and dirnames and filenames are lists of directories and files in
    currentUrl.

    If url does not exist, raises an exception.  For example:

        walk('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/supplementary/samples/GSM579') ->
        ftplib.error_perm: 550 /pub/geo/DATA/supplementary/samples/GSM579: No such file or directory

    :param url: An valid ftp url pointing to a directory.
    :param depth: By default or if depth < 0, walk searches
    all levels beneath the current directory specified by url.  Use depth to
    search only <depth> levels beneath the current dir.  If depth is 0, only
    yield the files in the directory specified by url.
    :param conn: An open, logged in ftp connection.  If None, a connection to
    url will be opened (and closed).  Conn should be open to the same host
    as the hostname in url.
    :param pause: How long to pause between listing each directory.  Defaults
    to no pause.  This is useful for throttling requests to the server.

    Example:

        print list(walk("ftp://ftp.ncbi.nlm.nih.gov/pub/geo", depth=1))'
        [('ftp://ftp.ncbi.nlm.nih.gov/pub/geo', ['DATA'], ['README.TXT']),
        ('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA', ['MINiML', 'SOFT',
        'SeriesMatrix', 'annotation', 'projects', 'roadmapepigenomics',
        'supplementary'], ['datasets_gi.txt.gz'])]
    '''
    with connect_and_login(url, ftp=conn, username=username, password=password) as ftp:
        url, dirs, files = listdir(url, conn=ftp)
        yield url, dirs, files
        # throttle ftp server requests
        time.sleep(pause)

        if depth == 0:
            return

        for dirname in dirs:
            sub_url = url + ('/' if not url.endswith('/') else '') + dirname
            for retval in walk(sub_url, depth=(depth - 1), conn=ftp,
                                pause=pause):
                yield retval


def isdir(url, conn=None, username=None, password=None):
    '''
    Return True or False depending on whether or not the path component of the
    url refers to a directory (as determined by CWD <path>) on the ftp server
    the url refers to.

    :param url: An valid ftp url pointing to a directory.
    :param conn: An open, logged in ftp connection.  If None, a connection to
    url will be opened (and closed).  Conn should be open to the same host
    as the hostname in url.
    '''
    # get path from url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    with connect_and_login(url, ftp=conn, username=username, password=password) as ftp:
        try:
            ftp.cwd(path)
            return True
        except ftplib.error_perm as e:
            # http://cr.yp.to/ftp/cwd.html
            # "The server may reject a CWD request using code 550.  Most
            # servers reject CWD requests unless the pathname refers to an
            # accessible directory."
            msg = str(e).strip()
            if msg.startswith('550'):
                return False
            else:
                raise


def mlsd(url, conn=None, use_fact_dict=True, username=None, password=None):
    '''
    Change to the directory indicated by URL, list the directory using MLSD,
    and return a list, where each entry in the list is a tuple of the pathname
    returned by the MLSD command and the facts.  By default, the facts entry is
    a dict containing the keywords (lowercased) and values returned by the MLSD
    command.  If use_fact_dict is False, the facts entry is a list of tuples
    containing the keywords (lowercased) and values returned by the MLSD
    command.  This option exists to handle the case where multiple instances of
    the same keyword are returned by the MLSD command.

    :param url: An valid ftp url pointing to a directory.
    :param conn: An open, logged in ftp connection.  If None, a connection to
    url will be opened (and closed).  Conn should be open to the same host
    as the hostname in url.

    Example:

        mlsd("ftp://ftp.ncbi.nlm.nih.gov/pub/geo/")'
        [('.', {'unix.owner': '14', 'unix.mode': '0444', 'modify':
                '20120206193711', 'perm': 'fle', 'unix.group': '5007',
                'unique': '17U7D7C6', 'type': 'cdir', 'size': '4096'}),
         ('..', {'unix.owner': '14', 'unix.mode': '0444', 'modify':
                 '20130207193122', 'perm': 'fle', 'unix.group': '0',
                 'unique': '17U1080BC', 'type': 'pdir', 'size': '8192'}),
         ('DATA', {'unix.owner': '14', 'unix.mode': '0444', 'modify':
                   '20130212111247', 'perm': 'fle', 'unix.group':
                   '0', 'unique': '41U2', 'type': 'dir', 'size': '0'}),
         ('README.TXT', {'unix.owner': '14', 'unix.mode': '0444', 'modify':
                         '20100923150800', 'perm': 'adfr', 'unix.group':
                         '5007', 'unique': '17U7B62C', 'type': 'file',
                         'size': '7254'})]
    '''
    # A bit about parsing MLSD lines
    # http://tools.ietf.org/html/rfc3659#section-7
    # The set of facts must not contain any spaces anywhere inside it.
    # The facts are a series of keyword=value pairs each followed by semi-colon
    # (";") characters.  An individual fact may not contain a semi-colon in its
    # name or value.  The complete series of facts may not contain the space
    # character.

    # get path from url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    output = []
    def parse(line):
        # print line
        fact_list = []
        facts_str, pathname = line.split(' ', 1)
        # print 'facts_str, pathname =', facts_str, pathname
        while facts_str:
            fact, facts_str = facts_str.split(';', 1)
            keyword, value = fact.split('=')
            # print 'keyword, value =', keyword, value
            fact_list.append((keyword.lower(), value))
            # print 'added fact'
        facts = dict(fact_list) if use_fact_dict else fact_list
        output.append((pathname.strip(), facts))
        # print 'fact, facts_str =', fact, facts_str

    with connect_and_login(url, ftp=conn, username=username, password=password) as ftp:
        ftp.cwd(path)
        ftp.retrlines('MLSD', parse)

    return output



def listdir(url, conn=None, username=None, password=None):
    '''
    :param url: An valid ftp url pointing to a directory.
    :param conn: An open, logged in ftp connection.  If None, a connection to
    url will be opened (and closed).  Conn should be open to the same host
    as the hostname in url.

    Example:

        print listdir("ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/")'
        ('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/', ['MINiML', 'SOFT',
        'SeriesMatrix', 'annotation', 'projects', 'roadmapepigenomics',
        'supplementary'], ['datasets_gi.txt.gz'])
    '''
    # get path from url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    with connect_and_login(url, ftp=conn, username=username, password=password) as ftp:
        ftp.cwd(path)
        files = []
        dirs = []
        ftp.retrlines('LIST', _gather_dirs_and_files(dirs, files))
        return (url, dirs, files)


def _gather_dirs_and_files(dirs, files):
    '''
    Accumulator function to be used with ftplib.FTP.retrlines()
    '''
    def sub(line):
        if line.startswith('d'):
            dirname = line.strip().split(None, 8)[8]
            # ignore current and parent dirs
            if dirname != '.' and dirname != '..':
                dirs.append(dirname)
        elif line.startswith('-'):
            filename = line.strip().split(None, 8)[8]
            files.append(filename)
    return sub



