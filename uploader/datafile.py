# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Search

from datetime import datetime
from subprocess import call

import csv
import io
import os
import re
import traceback
import zipfile


class IndexNotEmptyError(ValueError):
    pass


class DataFile:
    def __init__(self, datafile):
        self.datafile = datafile
        self.mapping_file = self.get_mapping_file()
        self.is_zip_file = zipfile.is_zipfile(datafile)

    def get_mapping_file(self):
        file_extension = self.get_file_extension()
        current_dir = os.path.dirname(__file__)
        mapping_file = os.path.join(current_dir, '..', 'mappings', file_extension + '-template.json')
        return mapping_file

    def get_file_name(self):
        file_name = os.path.basename(self.datafile).split(".")[0]
        return file_name

    def get_file_extension(self):
        file_extension = os.path.basename(self.datafile).split(".")[1]
        return file_extension

    def get_file_object(self, **kwargs):
        """
        :param kwargs: dictionary to give encoding param
        :return: file object
        """
        if self.is_zip_file:
            zip_file_obj = zipfile.ZipFile(self.datafile, 'r')
            # it assumes that zip file has only one file
            file_name = zip_file_obj.namelist()[0]
            file_obj = io.TextIOWrapper(zip_file_obj.open(file_name, 'r'), **kwargs)
        else:
            file_obj = io.open(self.datafile, "r", **kwargs)

        return file_obj

    def load(self, client, index_name, chunk_size, threads, timeout):

        # Create index with mapping. If it already exists, ignore this
        client.indices.create(index=index_name, ignore=400, body=open(self.mapping_file, 'r').read())

        # check if it exists some document in index from this file
        file_name = '{0}.{1}'.format(self.get_file_name(), self.get_file_extension())
        es_query = Search(using=client, index=index_name).filter('term', path=file_name)[:0]
        result = es_query.execute()
        if result.hits.total != 0:
            raise IndexNotEmptyError('There are {0} documents from this file in the index'.format(result.hits.total))

        # Send docs to elasticsearch
        for success, info in parallel_bulk(client, self.make_docs(), thread_count=threads, chunk_size=chunk_size,
                                           request_timeout=timeout, index=index_name, doc_type='doc',
                                           raise_on_exception=False):
            if not success:
                print('Doc failed', info)

    def parser_row(self, row):
        raise NotImplementedError()

    # Yield all fields in file + path and timestamp
    def make_docs(self):
        with self.get_file_object(encoding='latin1') as f:
            reader = csv.DictReader(f, delimiter=str('|'))
            # skip header
            reader.next()
            for row in reader:
                try:
                    yield self.parser_row(row)
                except ValueError:
                    traceback.print_exc()

    def get_header(self):
        filename, extension = os.path.basename(self.datafile).split(".")
        return {
            'general': 'date|dayType|expeditionNumber|minExpeditionTime|maxExpeditionTime|averageExpeditionTime|licensePlateNumber|GPSPointsNumber|averageTimeBetweenGPSPoints|GPSNumberWithRoute|GPSNumberWithoutRoute|transactionNumber|transactionOnBusNumber|transactionOnMetroNumber|transactionOnTrainNumber|transactionOnBusStation|smartcardNumber|transactionWithRoute|transactionWithoutRoute|stagesWithBusAlighting|stagesWithMetroAlighting|stagesWithTrainAlighting|stagesWithBusStationAlighting|tripNumber|completeTripNumber|tripsWithOneStage|tripsWithTwoStages|tripsWithThreeStages|tripsWithFourStages|tripsWithFiveOrMoreStages|tripsWithOnlyMetro|tripsThatUseMetro|tripsWithoutLastAlighting|validTripNumber|averageTimeOfTrips|averageDistanceOfTrips|averageVelocityOfTrips|tripNumberInMorningRushHour|averageTimeInMorningRushTrips|averageDistanceInMorningRushTrips|averageVelocityInMorningRushTrips|tripNumberInAfternoonRushHour|averageTimeInAfternoonRushTrips|averageDistanceInAfternoonRushTrips|averageVelocityInAfternoonRushTrips',
            'travel': 'id|tipodia|factor_expansion|n_etapas|tviaje|distancia_eucl|distancia_ruta|tiempo_subida|tiempo_bajada|mediahora_subida|mediahora_bajada|periodo_subida|periodo_bajada|tipo_transporte_1|tipo_transporte_2|tipo_transporte_3|tipo_transporte_4|srv_1|srv_2|srv_3|srv_4|paradero_subida|paradero_bajada|comuna_subida|comuna_bajada|zona_subida|zona_bajada',
            'od': 'date|dateType|authRouteCode|operator|userRouteCode|timePeriodInStopTime|startStopOrder|endStopOrder|authStartStopCode|authEndStopCode|userStartStopCode|userEndStopCode|startStopName|endStopName|startZone|endZone|tripNumber|tripWithoutLanding|expandedTripNumber',
        }[extension]

    def get_path(self):
        return os.path.basename(self.datafile)

    def name_to_date(self):
        file_name = self.get_file_name()
        start_date = datetime.strptime(file_name,
                                       '%Y-%m-%d').isoformat() + '.000Z'  # Python doesn't support military Z.
        return start_date


def get_timestamp():
    return datetime.utcnow()
