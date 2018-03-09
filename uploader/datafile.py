# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Search

from datetime import datetime

import csv
import io
import os
import traceback
import zipfile


class IndexNotEmptyError(ValueError):
    pass


class DataFile:
    def __init__(self, datafile):
        self.datafile = datafile
        self.basename = os.path.basename(self.datafile).replace('.zip', '')
        self.mapping_file = self.get_mapping_file()
        self.fieldnames = []

    def get_mapping_file(self):
        file_extension = self.get_file_extension()
        current_dir = os.path.dirname(__file__)
        mapping_file = os.path.join(current_dir, '..', 'mappings', file_extension + '-template.json')
        return mapping_file

    def get_file_name(self):
        file_name = os.path.basename(self.datafile).split(".")[0]
        return file_name

    def get_file_extension(self):
        file_extension = os.path.basename(self.datafile).split(".")[1]
        return file_extension

    def get_file_object(self, **kwargs):
        """
        :param kwargs: dictionary to give encoding param
        :return: file object
        """
        if zipfile.is_zipfile(self.datafile):
            zip_file_obj = zipfile.ZipFile(self.datafile, 'r')
            # it assumes that zip file has only one file
            file_name = zip_file_obj.namelist()[0]
            file_obj = io.TextIOWrapper(zip_file_obj.open(file_name, 'r'), **kwargs)
        else:
            file_obj = io.open(self.datafile, str('rb'), **kwargs)

        return file_obj

    def load(self, client, index_name, chunk_size, threads, timeout):

        # Open and store mapping
        mapping = open(self.mapping_file, 'r')

        # Create index with mapping. If it already exists, ignore this
        client.indices.create(index=index_name, ignore=400, body=mapping.read())

        # Close mapping file
        mapping.close()

        # check if it exists some document in index from this file
        es_query = Search(using=client, index=index_name).filter('term', path=self.basename)[:0]
        result = es_query.execute()
        if result.hits.total != 0:
            raise IndexNotEmptyError('There are {0} documents from this file in the index'.format(result.hits.total))

        # Send docs to elasticsearch
        for success, info in parallel_bulk(client, self.make_docs(), thread_count=threads, chunk_size=chunk_size,
                                           request_timeout=timeout, index=index_name, doc_type='doc',
                                           raise_on_exception=False):
            if not success:
                print('Doc failed', info)

    def row_parser(self, row, path, timestamp):
        raise NotImplementedError()

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            for row in reader:
                try:
                    # add path and timestamp
                    path = self.basename
                    timestamp = get_timestamp()
                    # yield fields
                    yield {"_source": self.row_parser(row, path, timestamp)}
                except ValueError:
                    traceback.print_exc()

    def name_to_date(self):
        file_name = self.get_file_name()
        # Python doesn't support Zulu time.
        start_date = datetime.strptime(file_name, '%Y-%m-%d').isoformat() + '.000Z'

        return start_date


def get_timestamp():
    return datetime.utcnow()
