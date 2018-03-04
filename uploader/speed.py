# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from uploader.datafile import DataFile, get_timestamp

import csv


class SpeedFile(DataFile):
    """ Class that represents a speed file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def make_docs(self):
        with self.get_file_object(encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = self.get_path()
                timestamp = get_timestamp()
                merged = str(row['route'] + '-' + row['section'] + '-' + row['periodId'])
                yield {"_source": dict(timestamp=timestamp, path=path, merged=merged, **row)}

    def get_header(self):
        return 'route|section|date|periodId|dayType|totalDistance|totalTime|speed|nObs|nInvalidObs'
