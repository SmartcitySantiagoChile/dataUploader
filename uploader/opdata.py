# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import traceback
from itertools import groupby

from dataUploader.uploader.datafile import DataFile, get_timestamp


class OPDataFile(DataFile):
    """ Class that represents a opdata file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['opRouteCode', 'operator', 'userRouteCode', 'direction', 'dayType', 'timePeriod',
                           'startPeriodTime', 'endPeriodTime', 'frecuency', 'capacity', 'distance', 'speed']

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            # Group data using 'route' as key
            for identifier, dayTypes in groupby(reader, lambda dayType: (dayType['dayType'], dayType['opRouteCode'],
                                                                         dayType['operator'],
                                                                         dayType['userRouteCode'])):
                try:
                    dayTypes = list(dayTypes)
                    path = self.basename
                    timestamp = get_timestamp()
                    dayTypes = [{
                        'timePeriod': int(p['timePeriod']),
                        'startPeriodTime': p['startPeriodTime'],
                        'endPeriodTime': p['endPeriodTime'],
                        'frecuency': float(p['frecuency']),
                        'capacity': float(p['capacity']),
                        'distance': float(p['distance']),
                        'speed': float(p['speed'])} for p in dayTypes]
                    yield {
                        "_source": {
                            "path": path,
                            "date": timestamp,
                            "opRouteCode": identifier[1],
                            "operator": int(identifier[2]),
                            "userRouteCode": identifier[3],
                            "dayType": dayTypes
                        }
                    }
                except ValueError:
                    traceback.print_exc()
