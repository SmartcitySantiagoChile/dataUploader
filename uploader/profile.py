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
                           'transactions', 'halfHourInStartTime', 'halfHourInStopTime', 'notValid',
                           'expandedEvasionBoarding', 'expandedEvasionAlighting',
                           'expandedBoardingPlusExpandedEvasionBoarding',
                           'expandedAlightingPlusExpandedEvasionAlighting', 'loadProfileWithEvasion',
                           'boardingWithAlighting'
                           ]

    def row_parser(self, row, path, timestamp):
        expedition_stop_time = row['expeditionStopTime']
        time_period_in_stop_time = row['timePeriodInStopTime']
        half_hour_in_stop_time = -1 if row['halfHourInStopTime'] is None else row['halfHourInStopTime']
        expanded_evasion_boarding = -1 if row['expandedEvasionBoarding'] is None else row['expandedEvasionBoarding']
        expanded_evasion_alighting = -1 if row['expandedEvasionAlighting'] is None else row['expandedEvasionAlighting']
        expanded_boarding_plus_expanded_evasion_boarding = -1 if row[
                                                                     'expandedBoardingPlusExpandedEvasionBoarding'] is None else \
            row['expandedBoardingPlusExpandedEvasionBoarding']
        expanded_alighting_plus_expanded_evasion_alighting = -1 if row[
                                                                       'expandedAlightingPlusExpandedEvasionAlighting'] is None else \
            row['expandedAlightingPlusExpandedEvasionAlighting']
        load_profile_with_evasion = -1 if row['loadProfileWithEvasion'] is None else row['loadProfileWithEvasion']
        boarding_with_alighting = -1 if row['boardingWithAlighting'] is None else row['boardingWithAlighting']
        if expedition_stop_time == '-':
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
            "notValid": int(row['notValid']),
            "expandedEvasionBoarding": float(expanded_evasion_boarding),
            "expandedEvasionAlighting": float(expanded_evasion_alighting),
            "expandedBoardingPlusExpandedEvasionBoarding": float(expanded_boarding_plus_expanded_evasion_boarding),
            "expandedAlightingPlusExpandedEvasionAlighting": float(expanded_alighting_plus_expanded_evasion_alighting),
            "loadProfileWithEvasion": float(load_profile_with_evasion),
            "boardingWithAlighting": float(boarding_with_alighting)
        }
