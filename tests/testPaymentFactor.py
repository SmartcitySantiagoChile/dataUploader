# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from unittest import TestCase

from dataUploader.uploader.paymentfactor import PaymentFactorFile

import mock
import os


class LoadProfileData(TestCase):

    def setUp(self):
        # default values
        self.index_name = 'paymentfactor'
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
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2019-08-10.paymentfactor')

        profile_uploader = PaymentFactorFile(file_path)
        list(profile_uploader.make_docs())

    @mock.patch('dataUploader.uploader.datafile.parallel_bulk')
    @mock.patch('dataUploader.uploader.datafile.Search')
    @mock.patch('dataUploader.loadData.Elasticsearch')
    def test_load_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = ['2019-08-10.paymentfactor', '2019-08-10.paymentfactor.gz',
                          '2019-08-10.paymentfactor.zip']
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), 'files', file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, 'info')]

            paymentfactor_uploader = PaymentFactorFile(file_path)
            paymentfactor_uploader.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads,
                                        self.timeout)

            list(paymentfactor_uploader.make_docs())

            parallel_bulk.assert_called()
