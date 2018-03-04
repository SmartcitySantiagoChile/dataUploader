# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from unittest import TestCase

from loadData import main, upload_file

import mock


class MainTestLoadData(TestCase):

    def setUp(self):
        pass

    @mock.patch('uploader.datafile.parallel_bulk')
    @mock.patch('uploader.datafile.Search')
    @mock.patch('loadData.argparse')
    @mock.patch('loadData.Elasticsearch')
    def test_load_profile_data_(self, elasticsearch_mock, argparse_mock, search_mock, parallel_bulk):
        #elasticsearch_mock
        argparse_mock.return_value = argparse_mock
        argparse_mock.ArgumentParser.return_value = argparse_mock
        argparse_mock.parse_args.return_value = argparse_mock
        file_path_pattern = 'tests/files/*.profile'
        type(argparse_mock).file = mock.PropertyMock(return_value=[file_path_pattern])

        search_mock.return_value = search_mock
        search_mock.execute.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        type(search_mock).filter = mock.PropertyMock(return_value=search_mock)
        type(search_mock).hits = mock.PropertyMock(return_value=search_mock)
        type(search_mock).total = mock.PropertyMock(return_value=0)

        parallel_bulk.return_value = [(True, 'info')]
        main()

