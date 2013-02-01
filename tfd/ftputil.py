

import contextlib
import ftplib
import time
import urlparse


def parse_netloc(netloc):
    '''
    Parse out and return a tuple of user, password, host, port from a network
    location.  Port is optional.  User is optional (but required if a password
    is given).  Password is optional.  Any missing components will be None
    in the returned tuple.  Note, if port is present, it is returned as an
    integer.

    netloc: In the form '[user[:password]@]host[:port]'
    '''
    username = password = host = port = None

    if '@' in netloc:
        userpass, hostport = netloc.split('@', 1)
    else:
        userpass = None
        hostport = netloc

    if userpass is not None:
        if ':' in userpass:
            username, password = userpass.split(':', 1)
        else:
            username = userpass

    if ':' in hostport:
        host, port = hostport.split(':', 1)
        try:
            port = int(port)
        except ValueError:
            raise Exception('Port is not an integer.', port)
        if port < 0:
            raise Exception('Port must be >= 0.', port)
    else:
        host = hostport

    return username, password, host, port


@contextlib.contextmanager
def connect_and_login(url, ftp=None):
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
        # scheme://netloc/path;parameters?query#fragment
        scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
        # username:password@host:port -> host
        username, password, host, port = parse_netloc(netloc)

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



def walk(url, depth=-1, conn=None, pause=0):
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
    url will be opened (and closed).
    :param pause: How long to pause between listing each directory.  Defaults
    to no pause.  This is useful for throttling requests to the server.

    Example:

        print list(walk("ftp://ftp.ncbi.nlm.nih.gov/pub/geo", depth=1))'
        [('ftp://ftp.ncbi.nlm.nih.gov/pub/geo', ['DATA'], ['README.TXT']),
        ('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA', ['MINiML', 'SOFT',
        'SeriesMatrix', 'annotation', 'projects', 'roadmapepigenomics',
        'supplementary'], ['datasets_gi.txt.gz'])]
    '''
    with connect_and_login(url, ftp=conn) as ftp:
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


def listdir(url, conn=None):
    '''
    :param url: An valid ftp url pointing to a directory.
    :param conn: An open, logged in ftp connection.  If None, a connection to
    url will be opened (and closed).

    Example:

        print listdir("ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/")'
        ('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/', ['MINiML', 'SOFT',
        'SeriesMatrix', 'annotation', 'projects', 'roadmapepigenomics',
        'supplementary'], ['datasets_gi.txt.gz'])
    '''
    # get path from url
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)

    with connect_and_login(url, ftp=conn) as ftp:
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



