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
                           'subidas_evadidas', 'bajadas_evadidas',
                           'subidas_corregidas',
                           'bajadas_corregidas', 'carga_corregida',
                           'subidas_conbajada', '%evasion', 'tipo_evasion', 'uniforme',
                           'pax-km_tramo', 'pax-km_corregido_tramo', 'plazas-km_tramo'
                           ]

    a = ["Operador", "ServicioSentido", "ServicioUsuario", "Patente", "Paradero", "NombreParada", "Hini", "Hfin",
         "Cumplimiento", "Correlativo", "idExpedicion", "DistEnRuta", "#Subidas", "#SubidasLejanas", "Subidastotal",
         "SubidasExpandidas", "#Bajadas", "#BajadasLejanas", "Bajadastotal", "BajadasExpandidas", "Carga", "Capacidad",
         "TiempoGPSInterpolado", "TiempoPrimeraTrx", "TiempoGPSMasCercano", "Tiempo", "nSubidasTmp", "ParaderoUsuario",
         "PeriodoTSExpedicion", "PeriodoTSParada", "TipoDia", "ZP", "DeltaTrxs", "MHSalida", "MHPasada",
         "ExpedicionConProblema", "subidas_evadidas", "bajadas_evadidas", "subidas_corregidas", "bajadas_corregidas",
         "carga_corregida", "subidas_conbajada", "%evasion", "tipo_evasion", "uniforme", "pax-km_tramo",
         "pax-km_corregido_tramo", "plazas-km_tramo"]

    def row_parser(self, row, path, timestamp):
        expedition_stop_time = row['Tiempo']
        time_period_in_stop_time = row['PeriodoTSParada']
        half_hour_in_stop_time = -1 if row['MHPasada'] is None else row['MHPasada']
        expanded_evasion_boarding = 0 if row['subidas_evadidas'] is None else row['subidas_evadidas']
        expanded_evasion_alighting = 0 if row['bajadas_evadidas'] is None else row['bajadas_evadidas']
        expanded_boarding_plus_expanded_evasion_boarding = 0 if row['subidas_corregidas'] is None else \
            row['subidas_corregidas']
        expanded_alighting_plus_expanded_evasion_alighting = 0 if row['bajadas_corregidas'] is None else \
            row['bajadas_corregidas']
        load_profile_with_evasion = 0 if row['carga_corregida'] is None else row['carga_corregida']
        boarding_with_alighting = 0 if row['subidas_conbajada'] is None else row['subidas_conbajada']
        if expedition_stop_time == '-':
            time_period_in_stop_time = -1
            half_hour_in_stop_time = -1
        evasion_percent = 0 if row['%evasion'] is None else float(row['%evasion'])
        evasion_type = -1 if row['tipo_evasion'] is None else int(row['tipo_evasion'])
        uniform_distribution_method = -1 if row['uniforme'] is None else int(row['uniforme'])
        pax_km_section = 0 if row['pax-km_tramo'] is None or row['pax-km_tramo'] == '-' else float(row['pax-km_tramo'])
        pax_with_evasion_km_section = 0 if row['pax-km_corregido_tramo'] is None or row[
            'pax-km_corregido_tramo'] == '-' else float(row['pax-km_corregido_tramo'])
        capacity_km_section = 0 if row['plazas-km_tramo'] is None or row['plazas-km_tramo'] == '-' \
            else float(row['plazas-km_tramo'])

        return {
            'path': path,
            'timestamp': timestamp,
            'operator': int(row['Operador']),
            'route': row['ServicioSentido'],
            'userRoute': row['ServicioUsuario'],
            'licensePlate': row['Patente'],
            'authStopCode': row['Paradero'],
            'userStopName': row['NombreParada'],
            'expeditionStartTime': row['Hini'],
            'expeditionEndTime': row['Hfin'],
            'fulfillment': row['Cumplimiento'],
            'expeditionStopOrder': int(row['Correlativo']),
            'expeditionDayId': int(row['idExpedicion']),
            'stopDistanceFromPathStart': int(row['DistEnRuta']),
            'boarding': float(row['#Subidas']),
            'expandedBoarding': float(row['SubidasExpandidas']),
            'expandedAlighting': float(row['BajadasExpandidas']),
            'loadProfile': float(row['Carga']),
            'busCapacity': int(row['Capacidad']),
            'expeditionStopTime': expedition_stop_time,
            'userStopCode': row['ParaderoUsuario'],
            'timePeriodInStartTime': int(row['PeriodoTSExpedicion']),
            'timePeriodInStopTime': int(time_period_in_stop_time),
            'dayType': int(row['TipoDia']),
            'busStation': int(row['ZP']),
            'transactions': int(row['DeltaTrxs']),
            'halfHourInStartTime': int(row['MHSalida']),
            'halfHourInStopTime': int(half_hour_in_stop_time),
            'notValid': int(row['ExpedicionConProblema']),
            'expandedEvasionBoarding': float(expanded_evasion_boarding),
            'expandedEvasionAlighting': float(expanded_evasion_alighting),
            'expandedBoardingPlusExpandedEvasionBoarding': float(expanded_boarding_plus_expanded_evasion_boarding),
            'expandedAlightingPlusExpandedEvasionAlighting': float(expanded_alighting_plus_expanded_evasion_alighting),
            'loadProfileWithEvasion': float(load_profile_with_evasion),
            'boardingWithAlighting': float(boarding_with_alighting),
            'evasionPercent': evasion_percent,
            'evasionType': evasion_type,
            'uniformDistributionMethod': uniform_distribution_method,
            'passengerPerKmSection': pax_km_section,
            'passengerWithEvasionPerKmSection': pax_with_evasion_km_section,
            'capacityPerKmSection': capacity_km_section,
        }
