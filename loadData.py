#!/usr/bin/python3
# This script will create an index on a existing elasticsearch instance and populate it using data from a given file.
# If an index with the same name already exists, it will use that index instead.

import argparse
import os

from elasticsearch import Elasticsearch

from datafile import DataFile
from expedition import ExpeditionFile
from profile import ProfileFile
from shape import ShapeFile
from speed import SpeedFile
from stop import StopFile

if __name__ == "__main__":
    # Arguments and description
    parser = argparse.ArgumentParser(description='Add documents from a file to an elasticsearch index.')
    parser.add_argument('-host', default="127.0.0.1", help='elasticsearch host, default is "127.0.0.1"')
    parser.add_argument('-port', default=9200, help='port, default is 9200')
    parser.add_argument('-index', help='name of the index to create/use')
    parser.add_argument('-file', help='data file path, e.g. /path/to/file')
    parser.add_argument('-chunk', default=5000, help='number of docs to send in one chunk, default is 5000')
    parser.add_argument('-threads', default=4, help='number of threads to use, default is 4')
    parser.add_argument('-timeout', default=30, help='explicit timeout for each call, default is 30 (seconds)')
    args = parser.parse_args()

    # Get a client
    es = Elasticsearch(hosts=[{"host": args.host, "port": args.port}])

    # Give names to arguments
    index = args.index
    datafile = args.file
    chunk_size = int(args.chunk)
    threads = int(args.threads)
    timeout = int(args.timeout)

    # Get filename and extension (replace for a switch/case or something)
    filename, file_extension = os.path.basename(datafile).split(".")

    # Determine file type according to the extension
    if file_extension == 'shape':
        fileToLoad = ShapeFile(datafile)
    elif file_extension == 'speed':
        fileToLoad = SpeedFile(datafile)
    elif file_extension == 'expedition':
        fileToLoad = ExpeditionFile(datafile)
    elif file_extension == 'profile':
        fileToLoad = ProfileFile(datafile)
    elif file_extension == 'stop':
        fileToLoad = StopFile(datafile)
    else:
        fileToLoad = DataFile(datafile)

    # Load file
    fileToLoad.load(es, index, chunk_size, threads, timeout)
