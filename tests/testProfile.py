import csv
import os
from unittest import TestCase, mock

from datauploader.uploader.datafile import DataFile
from datauploader.uploader.profile import ProfileFile


class LoadProfileData(TestCase):

    def setUp(self):
        # default values
        self.index_name = 'profile'
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
        file_path = os.path.join(os.path.dirname(__file__), 'files', '2020-11-30-with-evasion-2.profile')

        profile_uploader = ProfileFile(file_path)
        list(profile_uploader.make_docs())

    @mock.patch('datauploader.uploader.datafile.parallel_bulk')
    @mock.patch('datauploader.uploader.datafile.Search')
    @mock.patch('datauploader.loadData.Elasticsearch')
    def test_load_profile_data(self, elasticsearch_mock, search_mock, parallel_bulk):
        file_name_list = ['2020-11-30-with-evasion-2.profile', '2020-11-30-with-evasion-2.profile.gz',
                          '2020-11-30-with-evasion-2.profile.zip',
                          '2017-07-31-with-evasion.profile', '2017-07-31-with-evasion.profile.gz',
                          '2017-07-31-with-evasion.profile.zip', '2020-03-20.profile', '2020-03-20.profile.gz',
                          '2020-03-20.profile.zip', '2021-06-30.profile', '2021-06-30.profile.gz', '2021-06-30.profile.zip']
        for file_name in file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), 'files', file_name)
            self.prepare_search_mock(search_mock)
            parallel_bulk.return_value = [(True, 'info')]

            profile_uploader = ProfileFile(file_path)
            profile_uploader.load(elasticsearch_mock, self.index_name, self.chunk_size, self.threads, self.timeout)

            list(profile_uploader.make_docs())

            parallel_bulk.assert_called()

    def test_field_names(self):
        old_file_name_list = ['2020-11-30-with-evasion-2.profile', '2020-11-30-with-evasion-2.profile.gz',
                              '2020-11-30-with-evasion-2.profile.zip',
                              '2020-03-20.profile', '2020-03-20.profile.gz', '2020-03-20.profile.zip']
        current_file_name_list = ['2021-06-30.profile', '2021-06-30.profile.gz', '2021-06-30.profile.zip']

        for file_name in current_file_name_list + old_file_name_list:
            file_path = os.path.join(os.path.dirname(__file__), 'files', file_name)
            profile_uploader = ProfileFile(file_path)
            data_file = DataFile(file_path)
            with data_file.get_file_object() as csvfile:
                reader = csv.reader(csvfile, delimiter="|")
                row = next(reader)
                if file_name in current_file_name_list:
                    self.assertEqual(profile_uploader.fieldnames, row)
                else:
                    # old files don't have same header
                    self.assertTrue(all(col in profile_uploader.fieldnames for col in row))
