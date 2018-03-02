# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Search
from elasticsearch.exceptions import TransportError

from datetime import datetime
from subprocess import call

import csv
import io
import os
import re


class IndexNotEmptyError(ValueError):
    pass


class DataFile:
    def __init__(self, datafile):
        self.datafile = datafile
        self.mapping = self.get_mapping()

    def get_mapping(self):
        filename, file_extension = os.path.basename(self.datafile).split(".")
        current_dir = os.path.dirname(__file__)
        mapping_file = os.path.join(current_dir, '..', 'mappings', file_extension + '-template.json')
        return mapping_file

    def get_file_name_and_extension(self):
        file_name, file_extension = os.path.basename(self.datafile).split(".")
        return file_name, file_extension

    def load(self, client, index_name, chunk_size, threads, timeout):
        # check if exist some document in index from this file
        try:
            file_name = '{0}.{1}'.format(*self.get_file_name_and_extension())
            es_query = Search(using=client, index=index_name).filter('term', path=file_name)[:0]
            result = es_query.execute()
            if result.hits.total != 0:
                raise IndexNotEmptyError('There are {0} documents from this file in the index'.format(result.hits.total))
        except TransportError as e:
            if e.status_code != 404:
                raise e
        # The file needs to have the right header
        self.fix_header()
        # Create index with mapping. If it already exists, ignore this
        client.indices.create(index=index_name, ignore=400, body=open(self.mapping, 'r').read())
        # Send docs to elasticsearch
        for success, info in parallel_bulk(client, self.make_docs(), thread_count=threads, chunk_size=chunk_size,
                                           request_timeout=timeout, index=index_name, doc_type='doc',
                                           raise_on_exception=False):
            if not success: print('Doc failed', info)

    # Yield all fields in file + path and timestamp
    def make_docs(self):
        with io.open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = os.path.basename(self.datafile)
                timestamp = datetime.now()
                yield {"_source": dict(timestamp=timestamp, path=path, **row)}

    def get_header(self):
        filename, extension = os.path.basename(self.datafile).split(".")
        return {
            'general': 'date|dayType|expeditionNumber|minExpeditionTime|maxExpeditionTime|averageExpeditionTime|licensePlateNumber|GPSPointsNumber|averageTimeBetweenGPSPoints|GPSNumberWithRoute|GPSNumberWithoutRoute|transactionNumber|transactionOnBusNumber|transactionOnMetroNumber|transactionOnTrainNumber|transactionOnBusStation|smartcardNumber|transactionWithRoute|transactionWithoutRoute|stagesWithBusAlighting|stagesWithMetroAlighting|stagesWithTrainAlighting|stagesWithBusStationAlighting|tripNumber|completeTripNumber|tripsWithOneStage|tripsWithTwoStages|tripsWithThreeStages|tripsWithFourStages|tripsWithFiveOrMoreStages|tripsWithOnlyMetro|tripsThatUseMetro|tripsWithoutLastAlighting|validTripNumber|averageTimeOfTrips|averageDistanceOfTrips|averageVelocityOfTrips|tripNumberInMorningRushHour|averageTimeInMorningRushTrips|averageDistanceInMorningRushTrips|averageVelocityInMorningRushTrips|tripNumberInAfternoonRushHour|averageTimeInAfternoonRushTrips|averageDistanceInAfternoonRushTrips|averageVelocityInAfternoonRushTrips',
            'travel': 'id|tipodia|factor_expansion|n_etapas|tviaje|distancia_eucl|distancia_ruta|tiempo_subida|tiempo_bajada|mediahora_subida|mediahora_bajada|periodo_subida|periodo_bajada|tipo_transporte_1|tipo_transporte_2|tipo_transporte_3|tipo_transporte_4|srv_1|srv_2|srv_3|srv_4|paradero_subida|paradero_bajada|comuna_subida|comuna_bajada|zona_subida|zona_bajada',
            'od': 'date|dateType|authRouteCode|operator|userRouteCode|timePeriodInStopTime|startStopOrder|endStopOrder|authStartStopCode|authEndStopCode|userStartStopCode|userEndStopCode|startStopName|endStopName|startZone|endZone|tripNumber|tripWithoutLanding|expandedTripNumber',
        }[extension]

    def header_is_ok(self):
        # Read first line
        with io.open(self.datafile, 'r', encoding='latin-1') as f:
            header = f.readline().rstrip('\n')
        # If the header is already the one we want
        if header == self.get_header():
            return True
        else:
            return False

    def has_header(self):
        # Read first line
        with io.open(self.datafile, 'r', encoding='latin-1') as f:
            header = f.readline().rstrip('\n')
        # Check it only contains letters, spaces, |'s and #'s
        search = re.compile(r'[^a-zA-Z|# .]').search
        return not bool(search(header))

    def remove_header(self):
        # Remove first line of the file
        call(["sed", "-i", '1d', self.datafile])

    def add_header(self):
        # Put this on the first line
        call(["sed", "-i", '1i ' + self.get_header(), self.datafile])

    def fix_header(self):
        # Check if the file has a header
        if self.has_header():
            if self.header_is_ok():
                pass
            else:
                self.remove_header()
                self.add_header()
        else:
            self.add_header()

    def get_path(self):
        return os.path.basename(self.datafile)

    def name_to_date(self):
        file_name, _ = self.get_file_name_and_extension()
        start_date = datetime.strptime(file_name,
                                       '%Y-%m-%d').isoformat() + '.000Z'  # Python doesn't support military Z.
        return start_date


def get_timestamp():
    return datetime.utcnow()
