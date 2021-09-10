from datauploader.uploader.datafile import DataFile


class StageFile(DataFile):
    """ Class that represents a stage file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['operador', 'id_etapa', 'correlativo_viajes', 'correlativo_etapas', 'tipo_dia',
                           'tipo_transporte', 'fExpansionServicioPeriodoTS', 'tiene_bajada', 'tiempo2', 'tiempo_subida',
                           'tiempo_bajada', 'tiempo_etapa', 'media_hora_subida', 'media_hora_bajada', 'x_subida',
                           'y_subida', 'x_bajada', 'y_bajada', 'dist_ruta_paraderos', 'dist_eucl_paraderos',
                           'servicio_subida', 'servicio_bajada', 'parada_subida', 'parada_bajada', 'comuna_subida',
                           'comuna_bajada', 'zona_subida', 'zona_bajada', 'sitio_subida', 'fExpansionZonaPeriodoTS',
                           'tEsperaMediaIntervalo', 'periodoSubida', 'periodoBajada', 'tiempoIniExpedicion']

    def row_parser(self, row, path, timestamp):
        operator = self.translate_operator(row['operador'])
        boarding_time = row['tiempo_subida']
        stage_number = int(row['correlativo_etapas'])
        day_type = self.translate_day_type(row['tipo_dia'])
        authority_stop_code = row['parada_subida']
        half_hour_in_boarding_time = self.translate_half_hour(row['media_hora_subida'])
        stop_commune = self.translate_commune(row['comuna_subida'])
        expanded_boarding = float(row['fExpansionZonaPeriodoTS']) if row['fExpansionZonaPeriodoTS'] != '-' else 0
        time_period_in_boarding_time = self.translate_period(row['periodoSubida'])
        bus_station = 1 if row["tipo_transporte"] == "ZP" else 0
        return {
            'path': path,
            'operator': operator,
            'boardingTime': boarding_time,
            'stageNumber': stage_number,
            'dayType': day_type,
            'authStopCode': authority_stop_code,
            'halfHourInBoardingTime': half_hour_in_boarding_time,
            'boardingStopCommune': stop_commune,
            'expandedBoarding': expanded_boarding,
            'timePeriodInBoardingTime': time_period_in_boarding_time,
            'busStation': bus_station
        }

    def translate_day_type(self, value):
        if value == 'LABORAL':
            return 0
        elif value == 'SABADO':
            return 1
        elif value == 'DOMINGO':
            return 2
        else:
            raise ValueError('day type "{0}" is not valid'.format(value))

    def translate_half_hour(self, value):
        data = {
            '00:00:00': 0, '00:30:00': 1, '01:00:00': 2, '01:30:00': 3, '02:00:00': 4, '02:30:00': 5, '03:00:00': 6,
            '03:30:00': 7, '04:00:00': 8, '04:30:00': 9, '05:00:00': 10, '05:30:00': 11, '06:00:00': 12, '06:30:00': 13,
            '07:00:00': 14, '07:30:00': 15, '08:00:00': 16, '08:30:00': 17, '09:00:00': 18, '09:30:00': 19,
            '10:00:00': 20, '10:30:00': 21, '11:00:00': 22, '11:30:00': 23, '12:00:00': 24, '12:30:00': 25,
            '13:00:00': 26, '13:30:00': 27, '14:00:00': 28, '14:30:00': 29, '15:00:00': 30, '15:30:00': 31,
            '16:00:00': 32, '16:30:00': 33, '17:00:00': 34, '17:30:00': 35, '18:00:00': 36, '18:30:00': 37,
            '19:00:00': 38, '19:30:00': 39, '20:00:00': 40, '20:30:00': 41, '21:00:00': 42, '21:30:00': 43,
            '22:00:00': 44, '22:30:00': 45, '23:00:00': 46, '23:30:00': 47
        }
        try:
            return data[value]
        except KeyError:
            raise ValueError('half hour "{0}" is not valid'.format(value))

    def translate_commune(self, value):
        data = {
            'LAMPA': 0, 'COLINA': 1, 'LO BARNECHEA': 2, 'LAS CONDES': 3, 'PENALOLEN': 4, 'LA FLORIDA': 5,
            'PUENTE ALTO': 6, 'SAN JOSE DE MAIPO': 7, 'SAN BERNARDO': 8, 'ISLA DE MAIPO': 9, 'TALAGANTE': 10,
            'CALERA DE TANGO': 11, 'PENAFLOR': 12, 'PADRE HURTADO': 13, 'MAIPU': 14, 'PUDAHUEL': 15,
            'ESTACION CENTRAL': 16, 'LO PRADO': 17, 'CERRO NAVIA': 18, 'RENCA': 19, 'QUILICURA': 20, 'HUECHURABA': 21,
            'VITACURA': 22, 'PROVIDENCIA': 23, 'LA REINA': 24, 'NUNOA': 25, 'MACUL': 26, 'SAN JOAQUIN': 27,
            'LA GRANJA': 28, 'LA PINTANA': 29, 'EL BOSQUE': 30, 'LO ESPEJO': 31, 'CERRILLOS': 32,
            'PEDRO AGUIRRE CERDA': 33, 'SANTIAGO': 34, 'QUINTA NORMAL': 35, 'INDEPENDENCIA': 36,
            'CONCHALI': 37, 'RECOLETA': 38, 'SAN MIGUEL': 39, 'SAN RAMON': 40, 'LA CISTERNA': 41
        }
        try:
            return data[value]
        except KeyError:
            raise ValueError('commune "{0}" is not valid'.format(value))

    def translate_period(self, value):
        data = {
            "01 - PRE NOCTURNO": 30,
            "02 - NOCTURNO": 31,
            "03 - TRANSICION NOCTURNO": 32,
            "04 - PUNTA MANANA": 33,
            "05 - TRANSICION PUNTA MANANA": 34,
            "06 - FUERA DE PUNTA MANANA": 35,
            "07 - PUNTA MEDIODIA": 36,
            "08 - FUERA DE PUNTA TARDE": 37,
            "09 - PUNTA TARDE1": 38,
            "10 - PUNTA TARDE2": 39,
            "11 - FUERA DE PUNTA NOCTURNO": 40,
            "12 - PRE NOCTURNO": 41,
            "01 - PRE NOCTURNO SABADO": 42,
            "02 - NOCTURNO SABADO": 43,
            "03 - TRANSICION SABADO MANANA": 44,
            "04 - PUNTA MANANA SABADO": 45,
            "05 - MANANA SABADO": 46,
            "06 - PUNTA MEDIODIA SABADO": 47,
            "07 - TARDE SABADO": 48,
            "08 - TRANSICION SABADO NOCTURNO": 49,
            "09 - PRE NOCTURNO SABADO": 50,
            "01 - PRE NOCTURNO DOMINGO": 51,
            "02 - NOCTURNO DOMINGO": 52,
            "03 - TRANSICION DOMINGO MANANA": 53,
            "04 - MANANA DOMINGO": 54,
            "05 - MEDIODIA DOMINGO": 55,
            "06 - TARDE DOMINGO": 56,
            "07 - TRANSICION DOMINGO NOCTURNO": 57,
            "08 - PRE NOCTURNO DOMINGO": 58
        }
        try:
            return data[value]
        except KeyError:
            raise ValueError(f'period "value" is not valid')

    def translate_operator(self, value):
        translated_value = int(value)
        if translated_value == 1:
            translated_value = 8
        if translated_value == 17:
            translated_value = 9
        return translated_value
