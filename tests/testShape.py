import os
from unittest import TestCase, mock

import mock

from datauploader.uploader.shape import ShapeFile


class LoadShapeData(TestCase):

    def setUp(self):
        # default values
        self.index_name = 'shape'
        self.chunk_size = 5000
        self.threads = 4
        self.timeout = 30

    def prepare_search_mock(self, search_mock):
        search_mock.return_value = search_mock
        search_mock.execute.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        type(search_mock).filter = mock.PropertyMock(return_value=search_mock)
        type(search_mock).hits = mock.PropertyMock(return_value=search_mock)
        type(search_mock).total = mock.PropertyMock(return_value=0)

    def test_check_make_docs(self):
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2017-04-03.shape')

        shape_uploader = ShapeFile(file_path)
        list(shape_uploader.make_docs())

    @mock.patch('datauploader.uploader.datafile.parallel_bulk')
    @mock.patch('datauploader.uploader.datafile.Search')
    @mock.patch('datauploader.loadData.Elasticsearch')
    def test_load_shape_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = ['2017-04-03.shape', '2017-04-03.shape.gz', '2017-04-03.shape.zip']
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), 'files', file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, 'info')]

            shape_uploader = ShapeFile(file_path)
            shape_uploader.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads, self.timeout)

            list(shape_uploader.make_docs())

            parallel_bulk.assert_called()
