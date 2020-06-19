# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from uploader.datafile import DataFile


class ProfileFile(DataFile):
    """ Class that represents a profile file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['operator', 'route', 'userRoute', 'licensePlate', 'authStopCode',
                           'userStopName', 'expeditionStartTime', 'expeditionEndTime', 'fulfillment',
                           'expeditionStopOrder', 'expeditionDayId', 'stopDistanceFromPathStart', '#Subidas',
                           '#SubidasLejanas', 'Subidastotal', 'expandedBoarding', '#Bajadas', '#BajadasLejanas',
                           'Bajadastotal', 'expandedAlighting', 'loadProfile', 'busCapacity', 'TiempoGPSInterpolado',
                           'TiempoPrimeraTrx', 'TiempoGPSMasCercano', 'expeditionStopTime', 'nSubidasTmp',
                           'userStopCode', 'timePeriodInStartTime', 'timePeriodInStopTime', 'dayType', 'busStation',
                           'transactions', 'halfHourInStartTime', 'halfHourInStopTime', 'notValid']

    def row_parser(self, row, path, timestamp):
        expedition_stop_time = row['expeditionStopTime']
        time_period_in_stop_time = row['timePeriodInStopTime']
        half_hour_in_stop_time = -1 if row['halfHourInStopTime'] is None else row['halfHourInStopTime']
        if expedition_stop_time == '-':
            expedition_stop_time = "0"
            time_period_in_stop_time = -1
            half_hour_in_stop_time = -1
        return {
            "path": path,
            "timestamp": timestamp,
            "operator": int(row['operator']),
            "route": row['route'],
            "userRoute": row['userRoute'],
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
            "timePeriodInStartTime": int(row['timePeriodInStartTime']),
            "timePeriodInStopTime": int(time_period_in_stop_time),
            "dayType": int(row['dayType']),
            "busStation": int(row['busStation']),
            "transactions": int(row['transactions']),
            "halfHourInStartTime": int(row['halfHourInStartTime']),
            "halfHourInStopTime": int(half_hour_in_stop_time),
            "notValid": int(row['notValid'])
        }
