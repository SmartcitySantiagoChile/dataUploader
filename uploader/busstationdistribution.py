# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from rqworkers.dataUploader.uploader.datafile import DataFile


class BusStationDistributionFile(DataFile):
    """ Class that represents a busstationdistribution file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['date', 'dayType', 'assignation', 'busStationId', 'busStationName', 'operator', 'total',
                           'sum', 'subtraction', 'neutral']

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "date": row['date'],
            "dayType": int(row['dayType']),
            "assignation": row['assignation'],
            "busStationId": row['busStationId'],
            "busStationName": row['busStationName'],
            "operator": int(row['operator']),
            "total": float(row['total']),
            "sum": float(row['sum']),
            "subtraction": float(row['subtraction']),
            "neutral": float(row['neutral'])
        }
