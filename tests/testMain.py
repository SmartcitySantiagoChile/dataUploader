import os
from unittest import TestCase, mock

from datauploader.loadData import main
from datauploader.uploader.datafile import DataFile


class Main(TestCase):
    def setUp(self):
        pass

    def set_search_mock(self, search_mock):
        search_mock.return_value = search_mock
        search_mock.execute.return_value = search_mock
        search_mock.__getitem__.return_value = search_mock
        type(search_mock).filter = mock.PropertyMock(return_value=search_mock)
        type(search_mock).hits = mock.PropertyMock(return_value=search_mock)
        type(search_mock).total = mock.PropertyMock(return_value=0)

    def set_argparse_mock(self, argparse_mock, file_path_pattern, index_name):
        argparse_mock.return_value = argparse_mock
        argparse_mock.ArgumentParser.return_value = argparse_mock
        argparse_mock.parse_args.return_value = argparse_mock
        type(argparse_mock).file = mock.PropertyMock(return_value=[file_path_pattern])
        type(argparse_mock).index = index_name

    @mock.patch("datauploader.uploader.datafile.parallel_bulk")
    @mock.patch("datauploader.uploader.datafile.Search")
    @mock.patch("datauploader.loadData.argparse")
    @mock.patch("datauploader.loadData.Elasticsearch")
    def test_load_file_data_(
        self, elasticsearch_mock, argparse_mock, search_mock, parallel_bulk
    ):
        pattern_list = [
            "*.profile",
            "*.expedition",
            "*.general",
            "*.od",
            "*.shape",
            "*.speed",
            "*.stop",
            "*.trip",
        ]
        for pattern in pattern_list:
            file_path_pattern = os.path.join(
                os.path.dirname(__file__), "files", pattern
            )
            argparse_mock.parse_args = argparse_mock
            type(argparse_mock).file = file_path_pattern
            self.set_argparse_mock(
                argparse_mock, file_path_pattern, pattern.split(".")[1]
            )
            self.set_search_mock(search_mock)

            parallel_bulk.return_value = [(True, "info")]
            main()

    @mock.patch("datauploader.uploader.datafile.parallel_bulk")
    @mock.patch("datauploader.uploader.datafile.Search")
    @mock.patch("datauploader.loadData.argparse")
    @mock.patch("datauploader.loadData.Elasticsearch")
    def test_load_zipped_file_data(
        self, elasticsearch_mock, argparse_mock, search_mock, parallel_bulk
    ):
        # elasticsearch_mock
        pattern_list = [
            "*.profile.zip",
            "*.expedition.zip",
            "*.general.zip",
            "*.od.zip",
            "*.shape.zip",
            "*.speed.zip",
            "*.stop.zip",
            "*.trip.zip",
        ]
        for pattern in pattern_list:
            file_path_pattern = os.path.join(
                os.path.dirname(__file__), "files", pattern
            )

            self.set_argparse_mock(
                argparse_mock, file_path_pattern, pattern.split(".")[1]
            )
            self.set_search_mock(search_mock)

            parallel_bulk.return_value = [(True, "info")]
            main()

    def test_get_int_value_or_minus_one(self):
        data_file_object = DataFile("")
        row_dict = {"example": 1, "example_2": "-"}
        self.assertEqual(
            data_file_object.get_int_value_or_minus_one(row_dict, "example"), 1
        )
        self.assertEqual(
            data_file_object.get_int_value_or_minus_one(row_dict, "example_3"), -1
        )
        self.assertEqual(
            data_file_object.get_int_value_or_minus_one(row_dict, "example_2"), -1
        )

    def test_get_float_value_or_minus_one(self):
        data_file_object = DataFile("")
        row_dict = {"example": 1.0, "example_2": "-"}
        self.assertEqual(
            data_file_object.get_float_value_or_minus_one(row_dict, "example"), 1.0
        )
        self.assertEqual(
            data_file_object.get_float_value_or_minus_one(row_dict, "example_3"), -1.0
        )
        self.assertEqual(
            data_file_object.get_float_value_or_minus_one(row_dict, "example_2"), -1.0
        )

    def test_get_string_value_or_hyphen(self):
        data_file_object = DataFile("")
        row_dict = {"example": "hello", "example_2": "-"}
        self.assertEqual(
            data_file_object.get_string_value_or_hyphen(row_dict, "example"), "hello"
        )
        self.assertEqual(
            data_file_object.get_string_value_or_hyphen(row_dict, "example_3"), "-"
        )
        self.assertEqual(
            data_file_object.get_string_value_or_hyphen(row_dict, "example_2"), "-"
        )
