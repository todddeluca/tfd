
'''
Utilities to assist with logging in python
'''

import logging
import os


class ConcurrentFileHandler(logging.Handler):
    """
    A handler class which writes logging records to a file.  Every time it
    writes a record it opens the file, writes to it, flushes the buffer, and
    closes the file.  Perhaps this could create problems in a very tight loop.
    This handler is an attempt to overcome concurrent write issues that
    the standard FileHandler has when multiple processes distributed across
    a cluster are all writing to the same log file.  Specifically, the records
    can become interleaved/garbled with one another.
    """
    def __init__(self, filename, mode="a"):
        """
        Open the specified file and use it as the stream for logging.

        :param mode: defaults to 'a', append.
        """
        logging.Handler.__init__(self)
        # keep the absolute path, otherwise derived classes which use this
        # may come a cropper when the current directory changes
        self.filename = os.path.abspath(filename)
        self.mode = mode


    def _openWriteClose(self, msg):
        f = open(self.filename, self.mode)
        f.write(msg)
        f.flush() # improves consistency of writes in a concurrent environment
        f.close()

    
    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline
        [N.B. this may be removed depending on feedback]. If exception
        information is present, it is formatted using
        traceback.print_exception and appended to the stream.
        """
        try:
            msg = self.format(record)
            fs = "%s\n"
            self._openWriteClose(fs % msg)
        except:
            self.handleError(record)


