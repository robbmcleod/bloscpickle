# coding=UTF-8
"""
Created on Wed Dec 28 11:47:37 2016

@author: Robert A. McLeod
"""

import bloscpickle
import json
import ujson
import rapidjson
import pickle
import marshal 
import msgpack

import os, os.path
import uuid
from time import time
MB = 2**20

from itertools import count; COUNTER = count()
import matplotlib.pyplot as plt
import numpy as np

####### MODULE TESTS #######
#from memory_profiler import profile
#@profile
def testUUID():
    """
    UUIDs are more or less random ascii codes, so they represent a difficult 
    target for data compression.
    """
    write = {}
    read = {}
    sizes = {}
    testDict = {}
    testDict['name'] = "Foo"
    testDict['id'] = next(COUNTER)
    testDict['uuCount'] = 2**18
    testDict['uuids'] = [str(uuid.uuid4()) for I in range( testDict['uuCount'] )]

    def execTest( testDict, pickler, useBlosc=False, 
                 compressor='zstd', clevel=1, shuffle=0, **pickler_kwargs ):
        
        if useBlosc:
            picklerName = 'blosc_{}{}_'.format(compressor,clevel) + pickler.__name__
            filename = "testfile." + picklerName
            
            with open( filename, 'wb' ) as stream:
                t0 = time()
                bloscpickle.dump(testDict, stream, pickler= pickler, 
                                 compressor=compressor, clevel=clevel, shuffle=shuffle, **pickler_kwargs )
                write[picklerName] = time() - t0
            with open( filename, 'rb' ) as stream:
                t1 = time()
                outDict = bloscpickle.load( stream, pickler=pickler )
                read[picklerName] = time() - t1
            if pickler is msgpack:
                print( "WARNING: {} failed in/out assert test".format(picklerName) )
            else:
                assert( testDict == outDict )
        else:
            picklerName = pickler.__name__
            filename = "testfile." + picklerName
            
            if pickler in (pickle,marshal,msgpack):
                with open( filename, 'wb' ) as stream:
                    t0 = time()
                    pickler.dump( testDict, stream, **pickler_kwargs )
                    write[picklerName] = time() - t0  
                
                with open( filename, 'rb' ) as stream:
                    t1 = time()
                    outDict = pickler.load( stream )
                    read[picklerName] = time() - t1
                try:
                    assert( testDict == outDict )
                except:
                    print( "WARNING: {} failed in/out assert test".format(picklerName) )
            else:
                with open( filename, 'w' ) as stream:
                    t0 = time()
                    pickler.dump( testDict, stream, **pickler_kwargs )
                    write[picklerName] = time() - t0  
                
                with open( filename, 'r' ) as stream:
                    t1 = time()
                    outDict = pickler.load( stream )
                    read[picklerName] = time() - t1
                try:
                    assert( testDict == outDict )
                except:
                    print( "WARNING: {} failed in/out assert test".format(picklerName) )
        
        sizes[picklerName] = os.path.getsize( filename ) / MB
        os.remove( filename )
        print( "{}:: write {:.2e} s, read {:.2e} s, size: {:.3f} MB"\
              .format( picklerName, write[picklerName], read[picklerName], sizes[picklerName] ) )
        return write[picklerName], sizes[picklerName]
    
    
    execTest( testDict, pickle )
    execTest( testDict, pickle, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, pickle, useBlosc=True, compressor='lz4', clevel=9 )
    
    execTest( testDict, marshal )
    execTest( testDict, marshal, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, marshal, useBlosc=True, compressor='lz4', clevel=9 )
    
    execTest( testDict, json, ensure_ascii=False )
    execTest( testDict, json, useBlosc=True, compressor='zstd', clevel=1, ensure_ascii=False )
    execTest( testDict, json, useBlosc=True, compressor='lz4', clevel=9, ensure_ascii=False )
    
    execTest( testDict, rapidjson )
    execTest( testDict, rapidjson, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, rapidjson, useBlosc=True, compressor='lz4', clevel=9 )
    
    execTest( testDict, ujson, ensure_ascii=False )
    execTest( testDict, ujson, useBlosc=True, compressor='zstd', clevel=1, ensure_ascii=False )
    execTest( testDict, ujson, useBlosc=True, compressor='lz4', clevel=9, ensure_ascii=False )
    
    execTest( testDict, msgpack, use_bin_type=False )
    execTest( testDict, msgpack, useBlosc=True, compressor='zstd', clevel=1, use_bin_type=False )
    execTest( testDict, msgpack, useBlosc=True, compressor='lz4', clevel=9, use_bin_type=False )
    
    uncompressed_writes = [write['pickle'], write['marshal'], write['json'], 
                          write['rapidjson'], write['ujson'], write['msgpack'] ]
    zstd_writes =  [write['blosc_zstd1_pickle'],write['blosc_zstd1_marshal'], write['blosc_zstd1_json'],
                    write['blosc_zstd1_rapidjson'], write['blosc_zstd1_ujson'], write['blosc_zstd1_msgpack'] ]
    lz4_writes =  [write['blosc_lz49_pickle'],write['blosc_lz49_marshal'], write['blosc_lz49_json'],
                    write['blosc_zstd1_rapidjson'], write['blosc_lz49_ujson'], write['blosc_lz49_msgpack'] ]


    indices = np.arange(6)
    bwidth = 0.25
    fig, ax = plt.subplots( figsize=(10,8) )
    bars_uncomp = ax.bar( indices, uncompressed_writes, bwidth, color='steelblue'  )
    bars_zstd = ax.bar( indices+bwidth, zstd_writes, bwidth, color='orange'  )
    bars_lz4 = ax.bar( indices+2*bwidth, lz4_writes, bwidth, color='purple'  )
    ax.set_ylabel( "Serialization write to disk time (s)" )
    ax.set_xticks( indices + 0.33 )
    ax.set_xticklabels( ('pickle','marshal','json','rapidjson','ujson','msgpack') ) 
    ax.legend( (bars_uncomp,bars_zstd,bars_lz4), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_uuid_writerate.png"  )
    
    uncompressed_reads = [read['pickle'], read['marshal'], read['json'], 
                          read['rapidjson'], read['ujson'], read['msgpack'] ]
    zstd_reads =  [read['blosc_zstd1_pickle'],read['blosc_zstd1_marshal'], read['blosc_zstd1_json'],
                   read['blosc_zstd1_rapidjson'], read['blosc_zstd1_ujson'], read['blosc_zstd1_msgpack'] ]
    lz4_reads =  [read['blosc_lz49_pickle'],read['blosc_lz49_marshal'], read['blosc_lz49_json'],
                  read['blosc_lz49_rapidjson'], read['blosc_lz49_ujson'], read['blosc_lz49_msgpack'] ]

    fig, ax = plt.subplots( figsize=(10,8) )
    bars_uncomp = ax.bar( indices, uncompressed_reads, bwidth, color='steelblue'  )
    bars_zstd = ax.bar( indices+bwidth, zstd_reads, bwidth, color='orange'  )
    bars_lz4 = ax.bar( indices+2*bwidth, lz4_reads, bwidth, color='purple'  )
    ax.set_ylabel( "Serialization read from disk time (s)" )
    #ax.set_title( "German dictionary with 326980 words" )
    ax.set_xticks( indices + 0.33 )
    ax.set_xticklabels( ('pickle','marshal','json', 'rapidjson', 'ujson','msgpack') ) 
    ax.legend( (bars_uncomp,bars_zstd,bars_lz4), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_uuid_readrate.png"  )
    
    
    
    uncompressed_sizes = [sizes['pickle'], sizes['marshal'], sizes['json'], 
                          sizes['rapidjson'], sizes['ujson'], sizes['msgpack'] ]
    zstd_sizes =  [sizes['blosc_zstd1_pickle'],sizes['blosc_zstd1_marshal'], sizes['blosc_zstd1_json'],
                    sizes['blosc_zstd1_rapidjson'], sizes['blosc_zstd1_ujson'], sizes['blosc_zstd1_msgpack'] ]
    lz4_sizes =  [sizes['blosc_lz49_pickle'],sizes['blosc_lz49_marshal'], sizes['blosc_lz49_json'],
                    sizes['blosc_lz49_rapidjson'], sizes['blosc_lz49_ujson'], sizes['blosc_lz49_msgpack'] ]

    fig2, ax2 = plt.subplots( figsize=(10,8) )
    bars_uncomp2 = ax2.bar( indices, uncompressed_sizes, bwidth, color='steelblue'  )
    bars_zstd2 = ax2.bar( indices+bwidth, zstd_sizes, bwidth, color='orange'  )
    bars_lz42 = ax2.bar( indices+2*bwidth, lz4_sizes, bwidth, color='purple'  )
    ax2.set_ylabel( "Disk usage (MB)" )
    ax2.set_xticks( indices + 0.33 )
    ax2.set_xticklabels( ('pickle','marshal','json','rapidjson','ujson','msgpack') ) 
    ax2.legend( (bars_uncomp2,bars_zstd2,bars_lz42), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_uuid_disksize.png"  )
    
def testJSON():
    """
    Compressing structured text dictionaries is a test case where we expect 
    compression to have a big impact.
    
    This requires a 'sample.json' file which is not included in the distribution.
    I recommend the following tool for generating sample JSON data:
        
        http://www.json-generator.com/
        
    It can be trivally modified to generate 10'000 entries.
    """
    write = {}
    read = {}
    sizes = {}
    with open( "sample.json", 'r' ) as jh:
        testDict = ujson.load( jh )
        
    def execTest( testDict, pickler, useBlosc=False, 
                 compressor='zstd', clevel=1, shuffle=0, **pickler_kwargs ):
        
        if useBlosc:
            picklerName = 'blosc_{}{}_'.format(compressor,clevel) + pickler.__name__
            filename = "testfile." + picklerName
            
            with open( filename, 'wb' ) as stream:
                t0 = time()
                bloscpickle.dump(testDict, stream, pickler= pickler, 
                                 compressor=compressor, clevel=clevel, shuffle=shuffle, **pickler_kwargs )
                write[picklerName] = time() - t0
            with open( filename, 'rb' ) as stream:
                t1 = time()
                outDict = bloscpickle.load( stream, pickler=pickler )
                read[picklerName] = time() - t1
            try:
                assert( testDict == outDict )
            except AssertionError:
                print( "WARNING: {} failed in/out assert test".format(picklerName) )
        else:
            picklerName = pickler.__name__
            filename = "testfile." + picklerName
            
            if pickler in (pickle,marshal,msgpack):
                with open( filename, 'wb' ) as stream:
                    t0 = time()
                    pickler.dump( testDict, stream, **pickler_kwargs )
                    write[picklerName] = time() - t0  
                
                with open( filename, 'rb' ) as stream:
                    t1 = time()
                    outDict = pickler.load( stream )
                    read[picklerName] = time() - t1
                try:
                    assert( testDict == outDict )
                except AssertionError:
                    print( "WARNING: {} failed in/out assert test".format(picklerName) )
            else:
                with open( filename, 'w' ) as stream:
                    t0 = time()
                    pickler.dump( testDict, stream, **pickler_kwargs )
                    write[picklerName] = time() - t0  
                
                with open( filename, 'r' ) as stream:
                    t1 = time()
                    outDict = pickler.load( stream )
                    read[picklerName] = time() - t1
                try:
                    assert( testDict == outDict )
                except AssertionError:
                    print( "WARNING: {} failed in/out assert test".format(picklerName) )
        
        sizes[picklerName] = os.path.getsize( filename ) / MB
        os.remove( filename )
        print( "{}:: write {:.2e} s, read {:.2e} s, size: {:.3f} MB"\
              .format( picklerName, write[picklerName], read[picklerName], sizes[picklerName] ) )
        return outDict
    
    
    execTest( testDict, pickle )
    execTest( testDict, pickle, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, pickle, useBlosc=True, compressor='lz4', clevel=9 )
    
    execTest( testDict, marshal )
    execTest( testDict, marshal, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, marshal, useBlosc=True, compressor='lz4', clevel=9 )
    
    execTest( testDict, json, ensure_ascii=False )
    execTest( testDict, json, useBlosc=True, compressor='zstd', clevel=1, ensure_ascii=False )
    execTest( testDict, json, useBlosc=True, compressor='lz4', clevel=9, ensure_ascii=False )
    
    execTest( testDict, rapidjson )
    execTest( testDict, rapidjson, useBlosc=True, compressor='zstd', clevel=1 )
    execTest( testDict, rapidjson, useBlosc=True, compressor='lz4', clevel=9 )
    
    # Testing here to try and find what broke on read/write for UltraJSON.
    # Probably floating point precision
    outDict = execTest( testDict, ujson, encode_html_chars=True )
    execTest( testDict, ujson, useBlosc=True, compressor='zstd', clevel=1, encode_html_chars=True )
    execTest( testDict, ujson, useBlosc=True, compressor='lz4', clevel=9, encode_html_chars=True )
    
    execTest( testDict, msgpack, use_bin_type=False )
    execTest( testDict, msgpack, useBlosc=True, compressor='zstd', clevel=1, use_bin_type=False )
    execTest( testDict, msgpack, useBlosc=True, compressor='lz4', clevel=9, use_bin_type=False )
    
    uncompressed_writes = [write['pickle'], write['marshal'], write['json'], 
                          write['rapidjson'], write['ujson'], write['msgpack'] ]
    zstd_writes =  [write['blosc_zstd1_pickle'],write['blosc_zstd1_marshal'], write['blosc_zstd1_json'],
                    write['blosc_zstd1_rapidjson'], write['blosc_zstd1_ujson'], write['blosc_zstd1_msgpack'] ]
    lz4_writes =  [write['blosc_lz49_pickle'],write['blosc_lz49_marshal'], write['blosc_lz49_json'],
                    write['blosc_zstd1_rapidjson'], write['blosc_lz49_ujson'], write['blosc_lz49_msgpack'] ]


    indices = np.arange(6)
    bwidth = 0.25
    fig, ax = plt.subplots( figsize=(10,8) )
    bars_uncomp = ax.bar( indices, uncompressed_writes, bwidth, color='steelblue'  )
    bars_zstd = ax.bar( indices+bwidth, zstd_writes, bwidth, color='orange'  )
    bars_lz4 = ax.bar( indices+2*bwidth, lz4_writes, bwidth, color='purple'  )
    ax.set_ylabel( "Serialization write to disk time (s)" )
    ax.set_xticks( indices + 0.33 )
    ax.set_xticklabels( ('pickle','marshal','json', 'rapidjson', 'ujson','msgpack') ) 
    ax.legend( (bars_uncomp,bars_zstd,bars_lz4), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_jsongen_writerate.png"  )
    
    uncompressed_reads = [read['pickle'], read['marshal'], read['json'], 
                          read['rapidjson'], read['ujson'], read['msgpack'] ]
    zstd_reads =  [read['blosc_zstd1_pickle'],read['blosc_zstd1_marshal'], read['blosc_zstd1_json'],
                   read['blosc_zstd1_rapidjson'], read['blosc_zstd1_ujson'], read['blosc_zstd1_msgpack'] ]
    lz4_reads =  [read['blosc_lz49_pickle'],read['blosc_lz49_marshal'], read['blosc_lz49_json'],
                  read['blosc_lz49_rapidjson'], read['blosc_lz49_ujson'], read['blosc_lz49_msgpack'] ]

    fig, ax = plt.subplots( figsize=(10,8) )
    bars_uncomp = ax.bar( indices, uncompressed_reads, bwidth, color='steelblue'  )
    bars_zstd = ax.bar( indices+bwidth, zstd_reads, bwidth, color='orange'  )
    bars_lz4 = ax.bar( indices+2*bwidth, lz4_reads, bwidth, color='purple'  )
    ax.set_ylabel( "Serialization read from disk time (s)" )
    #ax.set_title( "German dictionary with 326980 words" )
    ax.set_xticks( indices + 0.33 )
    ax.set_xticklabels( ('pickle','marshal','json', 'rapidjson', 'ujson','msgpack') ) 
    ax.legend( (bars_uncomp,bars_zstd,bars_lz4), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_jsongen_readrate.png"  )
    
    
    
    uncompressed_sizes = [sizes['pickle'], sizes['marshal'], sizes['json'], 
                          sizes['rapidjson'], sizes['ujson'], sizes['msgpack'] ]
    zstd_sizes =  [sizes['blosc_zstd1_pickle'],sizes['blosc_zstd1_marshal'], sizes['blosc_zstd1_json'],
                    sizes['blosc_zstd1_rapidjson'], sizes['blosc_zstd1_ujson'], sizes['blosc_zstd1_msgpack'] ]
    lz4_sizes =  [sizes['blosc_lz49_pickle'],sizes['blosc_lz49_marshal'], sizes['blosc_lz49_json'],
                    sizes['blosc_lz49_rapidjson'], sizes['blosc_lz49_ujson'], sizes['blosc_lz49_msgpack'] ]

    fig2, ax2 = plt.subplots( figsize=(10,8) )
    bars_uncomp2 = ax2.bar( indices, uncompressed_sizes, bwidth, color='steelblue'  )
    bars_zstd2 = ax2.bar( indices+bwidth, zstd_sizes, bwidth, color='orange'  )
    bars_lz42 = ax2.bar( indices+2*bwidth, lz4_sizes, bwidth, color='purple'  )
    ax2.set_ylabel( "Disk usage (MB)" )
    ax2.set_xticks( indices + 0.33 )
    ax2.set_xticklabels( ('pickle','marshal','json','rapidjson','ujson','msgpack') ) 
    ax2.legend( (bars_uncomp2,bars_zstd2,bars_lz42), ('uncompressed', 'zstd','lz4'), loc='best' )
    plt.savefig( "bloscpickle_jsongen_disksize.png"  )
    pass
    
if __name__ == "__main__":

    testUUID()
    testJSON()
    
        
