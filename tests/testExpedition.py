import csv
import os
from unittest import TestCase, mock

from datauploader.uploader.datafile import DataFile
from datauploader.uploader.expedition import ExpeditionFile


class LoadExpeditionData(TestCase):
    def setUp(self):
        # default values
        self.index_name = "expedition"
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
        file_path = os.path.join(
            os.path.dirname(__file__), "files", "2016-05-23.expedition"
        )

        expedition_uploader = ExpeditionFile(file_path)
        list(expedition_uploader.make_docs())

    @mock.patch("datauploader.uploader.datafile.parallel_bulk")
    @mock.patch("datauploader.uploader.datafile.Search")
    @mock.patch("datauploader.loadData.Elasticsearch")
    def test_load_expedition_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = [
            "2016-05-23.expedition",
            "2016-05-23.expedition.zip",
            "2016-05-23.expedition.gz",
        ]
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), "files", file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, "info")]

            expedition = ExpeditionFile(file_path)
            expedition.load(
                elasticsearch_mock,
                self.index_name,
                self.chunk_size,
                self.threads,
                self.timeout,
            )

            list(expedition.make_docs())

            parallel_bulk.assert_called()

    def test_field_names(self):
        file_name_list = [
            "2016-05-23.expedition",
            "2016-05-23.expedition.zip",
            "2016-05-23.expedition.gz",
        ]
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), "files", file_name)
            expedition_uploader = ExpeditionFile(file_path)
            data_file = DataFile(file_path)
            with data_file.get_file_object() as csvfile:
                reader = csv.reader(csvfile, delimiter="|")
                row = next(reader)
                self.assertEqual(expedition_uploader.fieldnames, row)
