import csv
import gzip
import io
import os
import traceback
import zipfile
from datetime import datetime

from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Search

from datauploader.config import BASE_DIR
from datauploader.errors import IndexNotEmptyError, StopDocumentExistError, FilterDocumentError


def is_gzipfile(file_path):
    with gzip.open(file_path) as file_obj:
        try:
            file_obj.read(1)
            return True
        except IOError:
            return False


class DataFile:
    def __init__(self, datafile):
        self.datafile = datafile
        self.basename = os.path.basename(self.datafile)
        self.fieldnames = []

    def get_mapping_file(self, index_name):
        mapping_file = os.path.join(BASE_DIR, 'mappings', '{0}-template.json'.format(index_name))
        return mapping_file

    def get_file_name(self):
        file_name = os.path.basename(self.datafile).split(".")[0]
        return file_name

    def get_file_object(self):
        """
        :return: file object
        """
        if zipfile.is_zipfile(self.datafile):
            zip_file_obj = zipfile.ZipFile(self.datafile)
            # it assumes that zip file has only one file
            file_name = zip_file_obj.namelist()[0]
            file_obj_0 = zip_file_obj.open(file_name, 'r')
            file_obj = io.TextIOWrapper(file_obj_0, encoding='utf-8')
        elif is_gzipfile(self.datafile):
            file_obj = gzip.open(self.datafile, str('rt'), encoding='utf-8')
        else:
            file_obj = io.open(self.datafile, str('r'), encoding='utf-8')

        return file_obj

    def load(self, client, index_name, chunk_size, threads, timeout):

        # Open and store mapping
        with open(self.get_mapping_file(index_name), 'r') as mapping:
            # Create index with mapping. If it already exists, ignore this
            client.indices.create(index=index_name, ignore=400, body=mapping.read())

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
            try:
                next(f)  # skip header
            except StopIteration:
                print("Error: file ", f.name, "is empty.")
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            for row in reader:
                try:
                    # add path and timestamp
                    path = self.basename
                    timestamp = get_timestamp()
                    # yield fields
                    yield {"_source": self.row_parser(row, path, timestamp)}
                except (StopDocumentExistError, FilterDocumentError):
                    pass
                except ValueError:
                    traceback.print_exc()

    def name_to_date(self):
        file_name = self.get_file_name()
        # Python doesn't support Zulu time.
        start_date = datetime.strptime(file_name, '%Y-%m-%d').isoformat() + '.000Z'

        return start_date

    def get_int_value_or_minus_one(self, row: dict, key: str) -> int:
        if row.get(key) and row.get(key) != "-":
            return int(row[key])
        return -1

    def get_float_value_or_minus_one(self, row: dict, key: str) -> float:
        if row.get(key) and row.get(key) != "-":
            return float(row[key])
        return -1.0

    def get_string_value_or_hyphen(self, row: dict, key: str) -> str:
        return row[key] if row.get(key) else "-"


def get_timestamp():
    return datetime.utcnow()
