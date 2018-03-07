# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from uploader.datafile import DataFile, get_timestamp

import csv
import traceback


class SpeedFile(DataFile):
    """ Class that represents a speed file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def make_docs(self):
        with open(self.datafile, "r", encoding="latin-1") as f:
            next(f) # skip header
            # get fieldnames
            fieldnames = self.get_fieldnames()
            reader = csv.DictReader(f, delimiter='|',
                                    fieldnames=fieldnames)
            print(reader.fieldnames)
            try:
                for row in reader:
                    path = self.get_path()
                    timestamp = get_timestamp()
                    merged = str(row['route'] + '-' + row['section'] + '-' + row['periodId'])
                    yield {"_source": {
                        "path": path,
                        "timestamp": timestamp,
                        "merged": merged,
                        **row
                    }}
            except ValueError:
                traceback.print_exc()

    def get_fieldnames(self):
        return ['route', 'section', 'date', 'periodId', 'dayType', 'totalDistance',
                                                'totalTime', 'speed', 'nObs', 'nInvalidObs']
