import csv
import os
from unittest import TestCase, mock

from datauploader.uploader.datafile import DataFile
from datauploader.uploader.paymentfactor import PaymentFactorFile


class LoadProfileData(TestCase):
    def setUp(self):
        # default values
        self.index_name = "paymentfactor"
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
        for file in ["2019-08-10.paymentfactor", "2022-06-22.paymentfactor"]:
            file_path = os.path.join(os.path.dirname(__file__), "files", file)

            profile_uploader = PaymentFactorFile(file_path)
            list(profile_uploader.make_docs())

    @mock.patch("datauploader.uploader.datafile.parallel_bulk")
    @mock.patch("datauploader.uploader.datafile.Search")
    @mock.patch("datauploader.loadData.Elasticsearch")
    def test_load_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = [
            "2019-08-10.paymentfactor",
            "2019-08-10.paymentfactor.gz",
            "2019-08-10.paymentfactor.zip",
            "2022-06-22.paymentfactor",
            "2022-06-22.paymentfactor.gz",
            "2022-06-22.paymentfactor.zip",
        ]
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), "files", file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, "info")]

            paymentfactor_uploader = PaymentFactorFile(file_path)
            paymentfactor_uploader.load(
                elasticsearch_mock,
                self.index_name,
                self.chunk_size,
                self.threads,
                self.timeout,
            )

            list(paymentfactor_uploader.make_docs())

            parallel_bulk.assert_called()

    def test_field_names(self):
        file_name_list = [
            "2019-08-10.paymentfactor",
            "2019-08-10.paymentfactor.gz",
            "2019-08-10.paymentfactor.zip",
            "2022-06-22.paymentfactor",
            "2022-06-22.paymentfactor.gz",
            "2022-06-22.paymentfactor.zip",
        ]
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), "files", file_name)
            paymentfactor_uploader = PaymentFactorFile(file_path)
            data_file = DataFile(file_path)
            with data_file.get_file_object() as csvfile:
                reader = csv.reader(csvfile, delimiter="|")
                row = next(reader)
                if len(row) == 16:
                    self.assertEqual(paymentfactor_uploader.fieldnames, row)
                else:
                    self.assertEqual(paymentfactor_uploader.fieldnames[0:14], row)
