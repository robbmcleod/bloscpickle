# -*- coding: utf-8 -*-
"""
bloscpickle

Created on Mon Dec 26 15:35:42 2016
@author: Robert A. McLeod
@email: robbmcleod@gmail.com

bloscpickle is a serialization interface that sits between the Python pickle 
library.  It leverages the blosc meta-compressor library to compress data 
before it is written to disk.

bloscpickle is compatible with the following Python serialization modules:
    
* `pickle`
* `marshal`
* `json`
* `jsonpickle`
* `ujson`
* `msgpack-python`


TODO: test for speed-ups on IO
TODO: test for speed-ups on Multiprocessing
    Unfortunately with the way Multiprocessing is written we can't easily 
    monkey patch it.  You would have to re-write this file:
        https://github.com/python/cpython/blob/master/Lib/multiprocessing/reduction.py
        
TODO: numpy integration
TODO: Can we pass blosc a ByteIO instead of a byte object?
"""
####### INITIALIZATION ON IMPORT ######
import sys # TODO: add support for Python 2.7
import pickle, json, marshal
import blosc
NOSHUFFLE = blosc.NOSHUFFLE
SHUFFLE = blosc.SHUFFLE
BISHUFFLE = blosc.BITSHUFFLE
from io import BytesIO, StringIO


# Build a dict that references all the pickling options we have available to us.
_picklers = {}
_picklers['pickle'] = pickle
_picklers['json'] = json
_picklers['marshal'] = marshal
try:
    import ujson
    _picklers['ujson'] =  ujson
except ImportError:
    pass

if sys.version_info < (2,8):
    try:
        import jsonpickler # Only compatible with Python 2.7
        _picklers['jsonpickler'] =  jsonpickler
        # jsonpickler is a little complicated by the fact it can handle multiple 
        # backends.
    except ImportError:
        pass
    
try:
    import msgpack
    _picklers['msgpack'] =  msgpack
except ImportError:
    pass

_defaultPickler = pickle
_defaultBlocksize = 65536
blosc.set_blocksize(_defaultBlocksize)
# Allow blosc to handle the default number of threads
_defaultCompressor = 'zstd'
_defaultCLevel = 1
_defaultShuffle = blosc.NOSHUFFLE
__version__ = "0.1.0.a0" 

####### SETTING MODULE LEVEL PARAMETERS ######
def set_pickler( pickler='pickle' ):
    global _defaultPickler
    try:
        _defaultPickler = _picklers[pickler]
    except:
        raise KeyError( "Unknown/unfound pickler: {}".format(pickler) )

def set_blocksize( blocksize=65536 ):
    blosc.set_blocksize( blocksize )
    
def set_nthreads( nthreads = 1 ):
    blosc.set_nthreads( nthreads )
    
def set_compressor( compressor='zstd' ):
    global _defaultCompressor
    _defaultCompressor = compressor
    
def set_clevel( clevel=1 ):
    global _defaultCLevel
    _defaultCLevel = clevel
    
def set_shuffle( shuffle=blosc.NOSHUFFLE ):
    global _defaultShuffle
    _defaultShuffle = shuffle
    

####### MODULE API #######
def dump( pyObject, stream, pickler=None, compressor=None, 
          clevel=None, shuffle=None, **pickler_args ):
    """
    Dump a Python object 'pyObject' into an io.IOBase subclass (typically 
    io.FileIO or io.BytesIO) as compressed bytes.
    
      pickler: { 'pickle','marshal','json','ujson','jsonpickle' }
      compressor: { 'zstd', 'lz4' } and others in the blosc library.
      clevel: {1 ... 9}, for compression level.  1 is advised for 'ztd' and 
        9 for 'lz4'.  'zstd' sees little benefit to compression ratio for 
        levels above 4.
      shuffle: {blosc.NOSHUFFLE,blosc.SHUFFLE,blosc.BITSHUFFLE}, re-orders the 
      data by most-significant byte or bit. For text data BITSHUFFLE is 
      recommended for compression ratio or NOSHUFFLE for speed.
      **pickle_args: are keyword arguments that will be passed to the called 
        'pickle'-style module, so refer to the documentation for those modules 
        for their particular keywords.  
    
    """
    if pickler is None: pickler = _defaultPickler
    if compressor is None: compressor = _defaultCompressor
    if clevel is None: clevel = _defaultCLevel
    if shuffle is None: shuffle = _defaultShuffle
    
    if pickler in (pickle, marshal, msgpack):
        bloscStream = BytesIO()
        pickler.dump( pyObject, bloscStream, **pickler_args )
        stream.write( blosc.compress( bloscStream.getvalue(), \
                    typesize=1, clevel=clevel, shuffle=shuffle, cname=compressor ) )
    else: # JSON works with Unicode, not Bytes
        bloscStream = StringIO()
        pickler.dump( pyObject, bloscStream, **pickler_args )
        stream.write( blosc.compress( bloscStream.getvalue().encode('utf-8'), \
                    typesize=1, clevel=clevel, shuffle=shuffle, cname=compressor ) )
        


def dumps(pyObject, pickler=None, compressor=None, 
          clevel=None, shuffle=None, **pickler_args ):
    """
    Dump a Python object 'pyObject' and returns a bytes object that has been
    compressed by blosc.
    
      pickler: { 'pickle','marshal','json','ujson','jsonpickle' }
      compressor: { 'zstd', 'lz4' } and others in the blosc library.
      clevel: {1 ... 9}, for compression level.  1 is advised for 'ztd' and 
        9 for 'lz4'.  'zstd' sees little benefit to compression ratio for 
        levels above 4.
      shuffle: {blosc.NOSHUFFLE,blosc.SHUFFLE,blosc.BITSHUFFLE}, re-orders the 
      data by most-significant byte or bit. For text data BITSHUFFLE is 
      recommended for compression ratio or NOSHUFFLE for speed.
      **pickler_args: are keyword arguments that will be passed to the called 
        'pickle'-style module, so refer to the documentation for those modules 
        for their particular keywords.  
    
    """
    if pickler is None: pickler = _defaultPickler
    if compressor is None: compressor = _defaultCompressor
    if clevel is None: clevel = _defaultCLevel
    if shuffle is None: shuffle = _defaultShuffle
    
    if pickler in (pickle, marshal, msgpack):
        bloscStream = BytesIO()
        pickler.dump( pyObject, bloscStream, **pickler_args )
        return blosc.compress( bloscStream.getvalue(), \
                    typesize=1, clevel=clevel, shuffle=shuffle, cname=compressor )
    else: # JSON works with Unicode, not Bytes
        bloscStream = StringIO()
        pickler.dump( pyObject, bloscStream, **pickler_args )
        return blosc.compress( bloscStream.getvalue().encode('utf-8'), \
                    typesize=1, clevel=clevel, shuffle=shuffle, cname=compressor )

def load( stream, pickler=None, **pickler_args ):
    """
    Reads an object from a open file-like object and returns it. 
    
    stream must have a read() method that returns a bytes string which is blosc 
    compressed data, with the appropriate blosc header.
    
      pickler: { 'pickle','marshal','json','ujson','jsonpickle' }
      **pickler_args: are keyword arguments that will be passed to the called 
        'pickle'-style module, so refer to the documentation for those modules 
        for their particular keywords.
    """
    if pickler is None: pickler = _defaultPickler
    
    if pickler in (pickle, marshal, msgpack):
        return pickler.loads( blosc.decompress( stream.read() ), **pickler_args )
    else:
        return pickler.loads( blosc.decompress( stream.read() ).decode(), **pickler_args )


def loads( bloscBytes, pickler=None, **pickler_args ):
    """
    Reads an object from a open file-like object and returns it. 
    
    bloscBytes must be a bytes string which is blosc compressed data, with the 
    appropriate blosc header.
    
      pickler: { 'pickle','marshal','json','ujson','jsonpickle' }
      **pickler_args: are keyword arguments that will be passed to the called 
        'pickle'-style module, so refer to the documentation for those modules 
        for their particular keywords.
    """
    if pickler is None: pickler = _defaultPickler
    
    if pickler in (pickle, marshal, msgpack):
        return pickler.loads( blosc.decompress( bloscBytes ), **pickler_args )
    else:
        return pickler.loads( blosc.decompress( bloscBytes ).decode(), **pickler_args )
