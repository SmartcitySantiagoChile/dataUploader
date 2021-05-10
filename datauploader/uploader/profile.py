from datauploader.uploader.datafile import DataFile


class ProfileFile(DataFile):
    """ Class that represents a profile file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['Operador', 'ServicioSentido', 'ServicioUsuario', 'Patente', 'Paradero',
                           'NombreParada', 'Hini', 'Hfin', 'Cumplimiento',
                           'Correlativo', 'idExpedicion', 'DistEnRuta', '#Subidas',
                           '#SubidasLejanas', 'Subidastotal', 'SubidasExpandidas', '#Bajadas', '#BajadasLejanas',
                           'Bajadastotal', 'BajadasExpandidas', 'Carga', 'Capacidad', 'TiempoGPSInterpolado',
                           'TiempoPrimeraTrx', 'TiempoGPSMasCercano', 'Tiempo', 'nSubidasTmp',
                           'ParaderoUsuario', 'PeriodoTSExpedicion', 'PeriodoTSParada', 'TipoDia', 'ZP',
                           'DeltaTrxs', 'MHSalida', 'MHPasada', 'ExpedicionConProblema',
                           'expandedEvasionBoarding', 'expandedEvasionAlighting',
                           'expandedBoardingPlusExpandedEvasionBoarding',
                           'expandedAlightingPlusExpandedEvasionAlighting', 'loadProfileWithEvasion',
                           'boardingWithAlighting', "evasionPercent", "evasionType", "uniformDistributionMethod"
                           ]

    def row_parser(self, row, path, timestamp):
        expedition_stop_time = row['Tiempo']
        time_period_in_stop_time = row['PeriodoTSParada']
        half_hour_in_stop_time = -1 if row['MHPasada'] is None else row['MHPasada']
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
            "operator": int(row['Operador']),
            "route": row['ServicioSentido'],
            "userRoute": row['ServicioUsuario'],
            "licensePlate": row['Patente'],
            "authStopCode": row['Paradero'],
            "userStopName": row['NombreParada'],
            "expeditionStartTime": row['Hini'],
            "expeditionEndTime": row['Hfin'],
            "fulfillment": row['Cumplimiento'],
            "expeditionStopOrder": int(row['Correlativo']),
            "expeditionDayId": int(row['idExpedicion']),
            "stopDistanceFromPathStart": int(row['DistEnRuta']),
            "expandedBoarding": float(row['SubidasExpandidas']),
            "expandedAlighting": float(row['BajadasExpandidas']),
            "loadProfile": float(row['Carga']),
            "busCapacity": int(row['Capacidad']),
            "expeditionStopTime": expedition_stop_time,
            "userStopCode": row['ParaderoUsuario'],
            "timePeriodInStartTime": int(row['PeriodoTSExpedicion']),
            "timePeriodInStopTime": int(time_period_in_stop_time),
            "dayType": int(row['TipoDia']),
            "busStation": int(row['ZP']),
            "transactions": int(row['DeltaTrxs']),
            "halfHourInStartTime": int(row['MHSalida']),
            "halfHourInStopTime": int(half_hour_in_stop_time),
            "notValid": int(row['ExpedicionConProblema']),
            "expandedEvasionBoarding": float(expanded_evasion_boarding),
            "expandedEvasionAlighting": float(expanded_evasion_alighting),
            "expandedBoardingPlusExpandedEvasionBoarding": float(expanded_boarding_plus_expanded_evasion_boarding),
            "expandedAlightingPlusExpandedEvasionAlighting": float(expanded_alighting_plus_expanded_evasion_alighting),
            "loadProfileWithEvasion": float(load_profile_with_evasion),
            "boardingWithAlighting": float(boarding_with_alighting),
            "evasionPercent": float(row['evasionPercent']),
            "evasionType": int(row['evasionType']),
            "uniformDistributionMethod": int(row['uniformDistributionMethod'])
        }
