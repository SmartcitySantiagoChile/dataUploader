# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import glob
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index

# add path so we can use function through command line
new_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(new_path)

from errors import UnrecognizedFileExtensionError, IndexNotEmptyError
from uploader.bip import BipFile
from uploader.paymentfactor import PaymentFactorFile
from uploader.expedition import ExpeditionFile
from uploader.general import GeneralFile
from uploader.odbyroute import OdByRouteFile
from uploader.profile import ProfileFile
from uploader.shape import ShapeFile
from uploader.speed import SpeedFile
from uploader.stop import StopFile
from uploader.stopbyroute import StopByRouteFile
from uploader.trip import TripFile
from uploader.opdata import OPDataFile


def upload_file(es_instance, datafile, index_name=None, chunk_size=5000, threads=4, timeout=30):
    """ upload file to elasticsearch """

    # Get file extension
    index_name = os.path.basename(datafile).split(".")[1] if index_name is None else index_name

    # Determine file type according to the extension
    if index_name == 'expedition':
        uploader = ExpeditionFile(datafile)
    elif index_name == 'general':
        uploader = GeneralFile(datafile)
    elif index_name == 'odbyroute':
        uploader = OdByRouteFile(datafile)
    elif index_name == 'profile':
        uploader = ProfileFile(datafile)
    elif index_name == 'shape':
        uploader = ShapeFile(datafile)
    elif index_name == 'speed':
        uploader = SpeedFile(datafile)
    elif index_name == 'stop':
        uploader = StopFile(datafile)
    elif index_name == 'stopbyroute':
        uploader = StopByRouteFile(datafile)
    elif index_name == 'trip':
        uploader = TripFile(datafile)
    elif index_name == 'paymentfactor':
        uploader = PaymentFactorFile(datafile)
    elif index_name == 'bip':
        uploader = BipFile(datafile)
    elif index_name == 'opdata':
        uploader = OPDataFile(datafile)
    else:
        raise UnrecognizedFileExtensionError(datafile)

    # Load file to elasticsearch
    uploader.load(es_instance, index_name, chunk_size, threads, timeout)


def main():
    """
    This script will create an index on a existing elasticsearch instance and populate it using data from a given
    file. If an index with the same name already exists, it will use that index instead.
    """

    # Arguments and description
    parser = argparse.ArgumentParser(description='Add documents from a file to an elasticsearch index.')

    parser.add_argument('file', nargs='*', help='data file path, e.g. /path/to/file')
    parser.add_argument('--host', default="127.0.0.1", help='elasticsearch host, default is "127.0.0.1"')
    parser.add_argument('--port', default=9200, help='port, default is 9200')
    parser.add_argument('--index', help='name of the index to create/use')
    parser.add_argument('--chunk', default=5000, type=int, help='number of docs to send in one chunk, default is 5000')
    parser.add_argument('--threads', default=4, type=int, help='number of threads to use, default is 4')
    parser.add_argument('--timeout', default=30, type=int,
                        help='explicit timeout for each call, default is 30 (seconds)')
    args = parser.parse_args()

    # Get a client
    es = Elasticsearch(hosts=[{"host": args.host, "port": args.port}])

    # Give names to arguments
    index_name = args.index
    datafiles = args.file
    chunk_size = args.chunk
    threads = args.threads
    timeout = args.timeout

    # disable refresh
    if index_name is not None:
        Index(index_name, using=es).put_settings(body={'index.refresh_interval': -1})

    for datafile in datafiles:
        matched_files = glob.glob(datafile)
        for matched_file in matched_files:
            print('uploading file {0}'.format(matched_file))
            try:
                upload_file(es, matched_file, index_name, chunk_size, threads, timeout)
            except IndexNotEmptyError as e:
                # ignore it and continue uploading files
                print('Error: {0}'.format(e))

    # enable refresh
    if index_name is not None:
        Index(index_name, using=es).put_settings(body={'index.refresh_interval': '1s'})


if __name__ == "__main__":
    main()
