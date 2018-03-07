# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from unittest import TestCase

from uploader.expedition import ExpeditionFile

import mock
import os


class LoadExpeditionData(TestCase):

    def setUp(self):
        # default values
        self.index_name = 'expedition'
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
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2016-03-14.travel')

        expedition_uploader = ExpeditionFile(file_path)
        list(expedition_uploader.make_docs())

    @mock.patch('uploader.datafile.parallel_bulk')
    @mock.patch('uploader.datafile.Search')
    @mock.patch('loadData.Elasticsearch')
    def test_load_expedition_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2016-03-14.travel')
        self.prepare_search_mock(search_mock)
        parallel_bulk.return_value = [(True, 'info')]

        expedition = ExpeditionFile(file_path)
        expedition.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads, self.timeout)

        list(expedition.make_docs())

        parallel_bulk.assert_called()

    @mock.patch('uploader.datafile.parallel_bulk')
    @mock.patch('uploader.datafile.Search')
    @mock.patch('loadData.Elasticsearch')
    def test_load_zipped_expedition_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2016-03-14.travel.zip')
        self.prepare_search_mock(search_mock)
        parallel_bulk.return_value = [(True, 'info')]

        expedition_uploader = ExpeditionFile(file_path)
        expedition_uploader.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads, self.timeout)

        list(expedition_uploader.make_docs())

        parallel_bulk.assert_called()
