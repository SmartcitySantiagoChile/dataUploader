# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
from unittest import TestCase

import mock

from uploader.bip import BipFile
from rqworkers.dataUploader.uploader.bip import BipFile


class LoadBipData(TestCase):

    def setUp(self):
        # default values
        self.index_name = 'bip'
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
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2019-10-07.bip')

        bip_uploader = BipFile(file_path)
        list(bip_uploader.make_docs())

    @mock.patch('uploader.datafile.parallel_bulk')
    @mock.patch('uploader.datafile.Search')
    @mock.patch('loadData.Elasticsearch')
    def test_load_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = ['2019-10-07.bip', '2019-10-07.bip.gz',
                          '2019-10-07.bip.zip']
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), 'files', file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, 'info')]

            bip_uploader = BipFile(file_path)
            bip_uploader.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads,
                                        self.timeout)

            list(bip_uploader.make_docs())

            parallel_bulk.assert_called()
