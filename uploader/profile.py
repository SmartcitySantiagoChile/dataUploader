# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from uploader.datafile import DataFile, get_timestamp


class ProfileFile(DataFile):
    """ Class that represents a profile file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['operator', 'route', 'userRoute', 'shapeRoute', 'licensePlate', 'authStopCode',
                           'userStopName', 'expeditionStartTime', 'expeditionEndTime', 'fulfillment',
                           'expeditionStopOrder', 'expeditionDayId', 'stopDistanceFromPathStart', '#Subidas',
                           '#SubidasLejanas', 'Subidastotal', 'expandedBoarding', '#Bajadas', '#BajadasLejanas',
                           'Bajadastotal', 'expandedAlighting', 'loadProfile', 'busCapacity', 'TiempoGPSInterpolado',
                           'TiempoPrimeraTrx', 'TiempoGPSMasCercano', 'expeditionStopTime', 'nSubidasTmp',
                           'userStopCode', 'timePeriodInStartTime', 'timePeriodInStopTime', 'dayType', 'busStation',
                           'transactions', 'halfHourInStartTime', 'halfHourInStopTime']

    def row_parser(self, row, path, timestamp):
        expedition_stop_time = row['expeditionStopTime']
        time_period_in_stop_time = row['timePeriodInStopTime']
        if expedition_stop_time == '-':
            expedition_stop_time = "0"
            time_period_in_stop_time = ""
        return {
            "path": path,
            "timestamp": timestamp,
            "operator": int(row['operator']),
            "route": row['route'],
            "userRoute": row['userRoute'],
            "shapeRoute": row['shapeRoute'],
            "licensePlate": row['licensePlate'],
            "authStopCode": row['authStopCode'],
            "userStopName": row['userStopName'],
            "expeditionStartTime": row['expeditionStartTime'],
            "expeditionEndTime": row['expeditionEndTime'],
            "fulfillment": row['fulfillment'],
            "expeditionStopOrder": int(row['expeditionStopOrder']),
            "expeditionDayId": int(row['expeditionDayId']),
            "stopDistanceFromPathStart": int(row['stopDistanceFromPathStart']),
            "expandedBoarding": float(row['expandedBoarding']),
            "expandedAlighting": float(row['expandedAlighting']),
            "loadProfile": float(row['loadProfile']),
            "busCapacity": int(row['busCapacity']),
            "expeditionStopTime": expedition_stop_time,
            "userStopCode": row['userStopCode'],
            "timePeriodInStartTime": row['timePeriodInStartTime'],
            "timePeriodInStopTime": time_period_in_stop_time,
            # TODO: make day_type int when it will convert to number type
            "dayType": row['dayType'],
            "busStation": int(row['busStation']),
            "transactions": int(row['transactions']),
            "halfHourInStartTime": int(row['halfHourInStartTime']),
            "halfHourInStopTime": int(row['halfHourInStopTime']) if row['halfHourInStopTime'] is not None else row[
                'halfHourInStopTime']
        }
