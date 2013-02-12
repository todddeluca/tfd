#!/usr/bin/env python

'''
General, widely applicable utilities.

ONLY DEPENDENCIES ON STANDARD LIBRARY MODULES ALLOWED IN THIS FILE.
'''

import datetime
import hashlib # sha
import itertools
import math
import os
import subprocess
import sys
import time


def coroutine(func):
    '''
    primes a coroutine function by calling next when the coroutine is first constructed.
    http://www.dabeaz.com/coroutines/
    '''
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start


def which(program):
    '''
    In python3.3, see shutil.which().  Emulate unix 'which' command.  If program
    contains any directories (e.g. './foo'), return program, else if program is
    found on the PATH, return the absolute path to it, otherwise return None.
    http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python/377028#377028
    '''
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


######################
# WORKING WITH NUMBERS

def isInteger(num):
    '''
    num: a thingy, e.g. string or number
    Tests if num represents an integer.  What is an integer?  In this case it
    is a thingy which can be converted to an integer and a float and when done
    so the two values are equal.
    Returns true if successful, false otherwise
    '''
    try:
        int(num)
        return int(num) == float(num)
    except (TypeError, ValueError):
        return False


def isNumber(num):
    '''
    num: possibly a number
    Attempts to convert num to a number.
    Returns true if successful, false otherwise
    '''
    try:
        float(num)
        return True
    except (TypeError, ValueError):
        return False

def humanBytes(num):
    '''
    http://blogmag.net/blog/read/38/Print_human_readable_file_size
    num: a number of bytes.
    returns a human-readable version of the number of bytes
    Byte (B), Kilobyte (KB), Megabyte (MB), Gigabyte (GB), Terabyte (TB), Petabyte (PB), Exabyte (EB), Zettabyte (ZB), Yottabyte (YB)
    '''
    for x in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0


def normsci(x): 
    '''
    Returns a tuple of the significand (i.e. mantissa) and exponent of `x` in
    base 10 in normalized scientific notation, a x 10**b, where 1 <= a < 10.
    See http://en.wikipedia.org/wiki/Scientific_notation#Normalized_notation.
    E.g. frexp10(897) -> (8.97, 2)
    E.g. frexp10(0.00897) -> (8.97, -3)
    Inspired by http://www.gossamer-threads.com/lists/python/python/867117
    '''
    # Implementation notes
    # log10(0.1) -> -1.0
    # log10(0.5) -> -0.3010299956639812 (need to round down)
    # log10(1.5) -> 0.17609125905568124 (need to round down)
    # log10(10) -> 1.0
    exp = int(math.floor(math.log10(x)))
    # make x a float so division works properly in python2
    return float(x) / 10**exp, exp 


############################

def run(args, stdin=None, shell=False):
    '''
    for python 2.7 and above, consider using subprocess.check_output().
    
    args: commandline string treated as a one element list, or list containing command and arguments.
    stdin: string to be sent to stdin of command.
    shell: defaults to False to avoid shell injection attacks
    
    Basically, if you want to run a command line, pass it as a string via args, and set shell=True.
    e.g. 'ls databases/fasta'
    If you do not want shell interpretation, break up the commandline and args into a list and set shell=False.
    e.g. ['ls', 'databases/fasta']
    Runs command, sending stdin to command (if any is given).  If shell=True, executes command through shell,
    interpreting shell characters in command and arguments.  If args is a string, runs args like a command line run
    on the shell.  If shell=False, executes command (the first item in args) with the other items in args as arguments.
    If args is a string, it is executed as a command.  If the string includes arguments, strange behavior will ensue.
    This is a convenience function wrapped around the subprocess module.

    returns: stdout of cmd (as string), if returncode is zero.
    if returncode is non-zero, throws Exception with the 'returncode' and 'stderr' of the cmd as attributes.
    '''
    p = subprocess.Popen(args, shell=shell, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = p.communicate(stdin)
    if p.returncode != 0:
        e = Exception('Error running command.  args='+str(args)+' returncode='+str(p.returncode)+'\nstdin='+str(stdin)+'\nstderr='+str(error))
        e.returncode = p.returncode
        e.stderr = error
        raise e
    else:
        return output


def dispatch(name, args=None, keywords=None):
    '''
    name: name of callable/function including modules, etc., e.g. 'foo_package.gee_package.bar_module.wiz_func'
    that can be imported from the current sys.path
    args: a list of arguments for the callable/function.
    keywords: a dict of keyword parameters for the callable/function.

    Using the fully qualifed name, loads module, finds and calls function with the given args and keywords
    returns: the return value of the called callable.
    '''
    if args is None:
        args = []
    if keywords is None:
        keywords = {}
    modname, attrname = name.rsplit(".", 1)
    __import__(modname)
    mod = sys.modules[modname]
    func = getattr(mod, attrname)
    return func(*args, **keywords)


# remove at will.
def testing(msg='Hello, World.'):
    print msg
    return msg


def strToBool(value, falsies=('F', 'FALSE', '0', '0.0', 'NO', 'N', 'NONE')):
    '''
    value: A string which will be interpreted as a boolean.
    falsies: a list of uppercase strings that are considered false.
    An arbitrary set of human-readable strings is mapped to False.  Everything else is true.
    What is false? Ingoring case, 'F', 'FALSE', '0', '0.0', 'NO', 'N', 'None'
    '''
    return str(value).upper() not in falsies


def getBoolFromEnv(key, default=True):
    '''
    looks in os.environ for key.  If key is set to 'F', 'False', '0', 'N', 'NO', or some other falsy value (case insensitive), returns false.
    Otherwise, if key is set, returns True.  Otherwise returns the default, which defaults to True.
    '''
    if os.environ.has_key(key):
        return strToBool(os.environ[key])
    else:
        return default


###########################
# CONTEXT MANAGER UTITLITES
###########################
# Generic context manager utilities.  Useful for turning objects or object factories into context managers.

class ClosingFactoryCM(object):
    '''
    context manager for creating a new obj from a factory function when entering a context an closing the obj when exiting a context.
    useful, for example, for creating and closing a db connection each time.
    Calls obj.close() when the context manager exits.
    '''
    def __init__(self, factory):
        self.factory = factory
        self.obj = None
        
    def __enter__(self):
        self.obj = None
        self.obj = self.factory()
        return self.obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.obj is not None:
            self.obj.close()


class FactoryCM(object):
    '''
    context manager for creating a new object from a factory function.  Might be useful for getting an object from a pool (e.g. a db connection pool).
    '''
    def __init__(self, factory):
        self.factory = factory

    def __enter__(self):
        return self.factory()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class NoopCM(object):
    '''
    context manager for getting an object.  Useful when a context manager is required instead of a simple object.  e.g. to reuse an existing db connection.
    '''
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


#######################################
# 
#######################################

def truePred(*args, **keywords):
    return True


def retryErrorExecute(operation, args=[], keywords={}, pred=truePred, numTries=1, delay=0, backoff=1):
    '''
    pred: function takes Exception as arg, returns True to retry operation, False otherwise.  Default is to
      always return True.
    numTries: number of times to try, including the first time. numTries < 0 means try an infinite # of times
      if numTries == 0, does not execute operation.  Simply returns.
    delay: pause (in seconds) between tries.  default = 0 (no delay)
    backoff: delay is multiplied by this factor after every retry, so the length of successive delays are
      delay, delay*backoff, delay*backoff*backoff, etc. default = 1 (no backoff)
    execute operation.  if an exception occurs, pass it to the predicate.  if the
    predicate returns true, retry the operation if there are any tries left.  Otherwise, raise the exception.  
    '''
    # could make backoff a function, so delay = backoff(delay), for more flexibility than just an exponential relationship.
    for i in xrange(numTries):
        try:
            return operation(*args, **keywords)
        except Exception, e:
            # re-raise exception if pred fails
            if not pred(e): raise
            # re-raise exception if that was the last try
            if i == (numTries-1): raise
            # else retry
            time.sleep(delay)
            delay *= backoff

# example:
# myFuncReturnValue = retryErrorExecute(myfunc, [param1, param2, param3], pred=customPred, numTries=10, delay=10, backoff=1.4)


################
# DATES AND TIME
################

def lastMonth(thisMonth=None):
    '''
    thisMonth: datetime or date obj.  defaults to today.
    returns: a date obj from the month before the month in thisMonth.  e.g. if this month is 2006/01/31, then 2005/12/01 is returned.
    '''
    if thisMonth == None:
        thisMonth = datetime.date.today()
    try:
        last = datetime.date(thisMonth.year, thisMonth.month-1, 1)
    except:
        last = datetime.date(thisMonth.year-1, 12, 1)
    return last


########
# RANDOM
########


class AttrDict(dict):
    '''
    A dictionary whose keys can also be accessed as items or attributes.
    This provides syntactic sugar to avoid typing lots of brackets and quotes.
    However by conflating attribute and item access semantics it can lead to
    strange behavior.

    Example:

        obj = AttrDict()
        obj['key1'] = 'hi'
        obj.key2 = 'hello'
        print obj['key1']
        print obj.key2

    Example of strange behavior when setting a built-in attribute:

        obj = AttrDict()
        obj.hi = 'hello!'
        print obj.get('hi') # 'hello!'
        obj.get = 'bye' # overwrite dict get function?  No.
        print obj.get # <built-in method get of AttrDict object at ...>
        print obj['get'] # 'bye'
        print obj.get('hi') # 'hello!'.  get() still works.
    '''
    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value



class Namespace(object):
    '''
    Use this if you want to instantiate an object to serve as a namespace.

    Example:
        foo = Namespace()
        # assignment
        foo.bar = 1
        # access
        print foo.bar; # prints '1'
        # testing for presence
        'bar' in foo # True
        hasattr(foo, 'bar') # True
        # iteration is a bit awkward
        for attrname in foo:
            value = getattr(foo, attrname)
            setattr(foo, attrname, 'Hi {}'.format(value))

    '''
    def __iter__(self):
        return iter(self.__dict__)


def mergeListOfLists(lists):
    '''
    lists: a list of lists
    returns: a list containing all the elements each list within lists
    '''
    merge = []
    for l in lists:
        merge.extend(l)
    return merge


def groupsOfN(iterable, n):
    '''
    iterable: some iterable collection
    n: length of lists to collect

    Iterates over iterable, returning lists of the next n element of iterable, until iterable runs out of elements.
    Last list may have less than n elements.
    returns: lists of n elements from iterable (except last list might have [0,n] elements.)
    '''
    seq = []
    count = 0
    it = iter(iterable)
    try:
        while 1:
            seq.append(it.next())
            count += 1
            if count == n:
                count = 0
                yield seq
                seq = []
    except StopIteration:
        if seq:
            yield seq


def splitIntoN(input, n, exact=False):
    '''
    input: a sequence
    n: the number of evenly-sized groups to split input into.  must be an integer > 0.
    exact: if True, returns exactly n sequences, even if some of them must be empty.
      if False, will not return empty sequences, so will return < n sequences when n > len(input).

    Split input into n evenly sized sequences.  If input is not evenly
    divisible, then the first sequences returned will have one more elements than
    later sequences.
    
    e.g. if input had 10 elements, [1,2,3,4,5,6,7,8,9,10] and n=3, input would be split into these sequences: [1,2,3,4],[5,6,7],[8,9,10]
    e.g. if input had 2 elements, [1,2] and n=3, input would be split into these sequences: [1], [2], []
    e.g. if input had 0 elements, [] and n=3, input would be split into these sequences: [], [], []
    e.g. if exact=False and input had 2 elements, [1,2] and n=3, input would be split into these sequences: [1], [2]
    e.g. if exact=False and input had 0 elements, [] and n=3, input would be split into no sequences.  I.e. no sequences would be yielded.
    yields: n evenly sized sequences, or if exact=False, up to n evenly sized, non-empty sequences.
    '''
    size = len(input) // n
    numExtra = len(input) % n
    start = 0
    end = size
    for i in range(n):
        if i < numExtra:
            end += 1
        if not exact and start == end: # only empty sequences left, so exit early.
            break
        yield input[start:end]
        start = end
        end = end + size



def makeCounter(n=0, i=1):
    '''
    n: number to start counting from
    i: increment
    This could be implemented the normal way using a closure, but python does not support writing to variables in a closure,
    forcing one to implement the function by wrapping the counter variable in a list.  I guess a generator is more pythonic.
    usage: counter = makeCounter(1); counter.next(); counter.next(); # etc.
    returns: a generator object which counts up from n by i, starting with n, when next() is called on the generator.
    '''
    while 1:
        yield n
        n = n + i

        
def every(pred, seq):
    """ returns True iff pred is True for every element in seq """
    for x in seq:
        if not pred(x): return False
    return True


def any(pred, seq):
    """ returns False iff pred is False for every element in seq """
    for x in seq:
        if pred(x): return True
    return False


def union(*iterables):
    '''
    Return a set which is the union of all the items in all the iterables.
    '''
    return set(item for it in iterables for item in it)


def intersection(*iterables):
    '''
    Return a set of the intersection of all the items in all the iterables.
    '''
    if len(iterables) == 1:
        return set(iterables[0])
    else:
        return set(iterables[0]).intersection(*iterables[1:])


########################
# DESCRIPTIVE STATISTICS
########################

# Need more speed and power: consider using scipy 

def mean(nums):
    return float(sum(nums))/len(nums)


def variance(nums):
    m = mean(nums)
    return sum([(n - m)**2 for n in nums]) / float(len(nums))


def stddev(nums):
    return math.sqrt(variance(nums))


def median(nums):
    l = len(nums)
    if l % 2 == 1: # odd
        return sorted(nums)[(l - 1) / 2] # the middle element
    else: # even
        s = sorted(nums)
        return (s[l / 2] + s[(l / 2) - 1]) / 2.0 # avg of 2 middle elements



#######################################
# COMBINATORICS FUNCTIONS
#######################################

def permute(items, n):
    '''
    deprecated: use itertools.permutations()
    returns a list of lists every permutation of n elements from items
    '''
    return list(itertools.permutations(items, n))


def choose(items, n):
    '''
    deprecated: use itertools.combinations()
    items: a list
    returns: a list of lists of every combination of n elements from items
    '''
    return list(itertools.combinations(items, n))


################################
# SERIALIZATION HELPER FUNCTIONS
################################

def loadObject(pickleFilename, protocol=-1, mode='rb'):
    '''
    use 'rb' mode for protocol 2, 'r' for protocol 0
    '''
    import cPickle
    fh = open(pickleFilename, mode)
    obj = cPickle.load(fh)
    fh.close()
    return obj


def dumpObject(obj, pickleFilename, protocol=-1, mode='wb'):
    '''
    use 'wb' mode for protocol 2, 'w' for protocol 0
    '''
    import cPickle
    fh = open(pickleFilename, mode)
    cPickle.dump(obj, fh, protocol=protocol)
    fh.close()
    return obj


################
# FILES AND DIRS 
################


def writeToFile(data, filename, mode='w'):
    '''
    opens file, writes data, and closes file
    flushing used to improve consistency of writes in a concurrent environment.
    '''
    fh = open(filename, mode)
    fh.write(data)
    fh.flush()
    fh.close()


def readFromFile(filename, mode='r'):
    '''
    opens file, reads data, and closes file
    returns: contents of file
    '''
    fh = open(filename, mode)
    data = fh.read()
    fh.close()
    return data


def differentFiles(filename1, filename2):
    '''
    compares the contents of the two files using the SHA digest algorithm.
    returns: True if the contents of the files are different.  False otherwise.
    throws: an exception if either file does not exist.
    '''
    file1 = open(filename1)
    file2 = open(filename2)

    s1 = hashlib.sha1() # sha.new()
    s2 = hashlib.sha1() # sha.new()
    for l in file1:
        s1.update(l)
    for l in file2:
        s2.update(l)
    isDifferent = (s1.hexdigest() != s2.hexdigest())

    file1.close()
    file2.close()
    return isDifferent


if __name__ == '__main__':
    pass

