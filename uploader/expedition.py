# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from uploader.datafile import DataFile, get_timestamp

import csv
import traceback


class ExpeditionFile(DataFile):
    """ Class that represents an expedition file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['route', 'licensePlate', 'a', 'b', 'expeditionStartTime', 'expeditionEndTime', 'fulfillment',
                           'c', 'd', 'e', 'f', 'g', 'h', 'periodId']

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "route": row['route'],
            "licensePlate": row['licensePlate'],
            "expeditionStartTime": row['expeditionStartTime'],
            "expeditionEndTime": row['expeditionEndTime'],
            "fulfillment": row['fulfillment'],
            "periodId": int(row['periodId'])
        }
