

import ftplib
import time
import urlparse


def walk(url, depth=-1, conn=None, pause=1.0):
    '''
    Traverse a directory tree rooted at url.  Similar to os.walk.  Walks the
    directories (in a depth-first manner) starting at url, yielding tuples of
    (currentUrl, dirnames, filenames), where currentUrl is url or a subdir of
    url, and dirnames and filenames are lists of directories and files in
    currentUrl.

    If url does not exist, raises an exception.  For example:

        walk('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/supplementary/samples/GSM579') ->
        ftplib.error_perm: 550 /pub/geo/DATA/supplementary/samples/GSM579: No such file or directory

    Example of a yielded tuple:

        walk('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/supplementary/samples/GSMnnn') ->
        ('ftp://ftp.ncbi.nlm.nih.gov/pub/geo/DATA/supplementary/samples/GSMnnn',
        ['GSM575', 'GSM576', 'GSM577', 'GSM578', 'GSM579'], [])

    :param depth: search <depth> levels beneath this one.  If depth is 0, only
    yield the files in the directory specified by url.  If depth is < 0, yield
    files and directories for every directory within url, including url itself.
    :param conn: An ftp connection.  If none, a connection to url will be opened (and closed).
    '''
    if url.endswith('/'):
        url = url[:-1]
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    # username:password@host:port -> host
    host = netloc.split('@')[-1].split(':')[0]

    close_conn = False
    if conn is None:
        conn = ftplib.FTP(host)
        close_conn = True
    try:
        conn.login()
        conn.cwd(path)
        files = []
        dirs = []
        if pause:
            time.sleep(pause)
        conn.retrlines('LIST', _gather_dirs_and_files(dirs, files))
        yield (url, dirs, files)
        if depth != 0:
            for dirname in dirs:
                sub_url = '/'.join([url, dirname])
                for retval in walk(sub_url, (depth-1), conn, pause):
                    yield retval
    except:
        if close_conn:
            conn.quit()
        raise


def _gather_dirs_and_files(dirs, files):
    '''
    Accumulator function to be used with ftplib.FTP.retrlines()
    '''
    def sub(line):
        if line.startswith('d'):
            dirname = line.split(None, 8)[8]
            # ignore current and parent dirs
            if dirname != '.' and dirname != '..':
                dirs.append(dirname)
        elif line.startswith('-'):
            filename = line.split(None, 8)[8]
            files.append(filename)
    return sub



