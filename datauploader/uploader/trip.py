from datauploader.uploader.datafile import DataFile


class TripFile(DataFile):
    """ Class that represents a trip file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['tipodia', 'factor_expansion', 'n_etapas', 'tviaje', 'distancia_eucl',
                           'distancia_ruta', 'tiempo_subida', 'tiempo_bajada', 'mediahora_subida', 'mediahora_bajada',
                           'periodo_subida', 'periodo_bajada', 'tipo_transporte_1', 'tipo_transporte_2',
                           'tipo_transporte_3', 'tipo_transporte_4', 'srv_1', 'srv_2', 'srv_3', 'srv_4',
                           'paradero_subida', 'paradero_bajada', 'comuna_subida', 'comuna_bajada', 'zona_subida',
                           'zona_bajada', 'modos', 'tiempo_subida_1', 'tiempo_bajada_1', 'tiempo_subida_2',
                           'tiempo_bajada_2', 'tiempo_subida_3', 'tiempo_bajada_3', 'tiempo_subida_4',
                           'tiempo_bajada_4', 'zona_subida_1', 'zona_bajada_1', 'zona_subida_2', 'zona_bajada_2',
                           'zona_subida_3', 'zona_bajada_3', 'zona_subida_4', 'zona_bajada_4', 'parada_subida_1',
                           'parada_subida_2', 'parada_subida_3', 'parada_subida_4', 'parada_bajada_1',
                           'parada_bajada_2', 'parada_bajada_3', 'parada_bajada_4', 'mediahora_bajada_1',
                           'mediahora_bajada_2', 'mediahora_bajada_3', 'mediahora_bajada_4', 'periodo_bajada_1',
                           'periodo_bajada_2', 'periodo_bajada_3', 'periodo_bajada_4']
        self.null_date = "1970-01-01 00:00:00"

    def row_parser(self, row, path, timestamp):
        row["path"] = path
        row["timestamp"] = timestamp
        row['tipodia'] = int(row['tipodia'])
        row['factor_expansion'] = float(row['factor_expansion'])
        row['n_etapas'] = int(row['n_etapas'])
        row['tviaje'] = float(row['tviaje'])
        row['distancia_eucl'] = float(row['distancia_eucl'])
        row['distancia_ruta'] = float(row['distancia_ruta'])
        row['tiempo_subida'] = row['tiempo_subida']
        row['tiempo_bajada'] = row['tiempo_bajada']
        row['mediahora_subida'] = int(row['mediahora_subida'])
        row['mediahora_bajada'] = int(row['mediahora_bajada'])
        row['periodo_subida'] = int(row['periodo_subida'])
        row['periodo_bajada'] = int(row['periodo_bajada'])
        row['tipo_transporte_1'] = int(row['tipo_transporte_1'])
        row['tipo_transporte_2'] = int(row['tipo_transporte_2'])
        row['tipo_transporte_3'] = int(row['tipo_transporte_3'])
        row['tipo_transporte_4'] = int(row['tipo_transporte_4'])
        if row['n_etapas'] == 1:
            row['servicio_bajada'] = row['srv_1']
        elif row['n_etapas'] == 2:
            row['servicio_bajada'] = row['srv_2']
        elif row['n_etapas'] == 3:
            row['servicio_bajada'] = row['srv_3']
        elif row['n_etapas'] == 4:
            row['servicio_bajada'] = row['srv_4']
        row['comuna_subida'] = int(row['comuna_subida'])
        row['comuna_bajada'] = int(row['comuna_bajada'])
        row['zona_subida'] = int(row['zona_subida'])
        row['zona_bajada'] = int(row['zona_bajada'])
        row['modos'] = int(row['modos'])
        row['tiempo_subida_1'] = row['tiempo_subida_1'] if row['tiempo_subida_1'] != '-' else self.null_date
        row['tiempo_bajada_1'] = row['tiempo_bajada_1'] if row['tiempo_bajada_1'] != '-' else self.null_date
        row['tiempo_subida_2'] = row['tiempo_subida_2'] if row['tiempo_subida_2'] != '-' else self.null_date
        row['tiempo_bajada_2'] = row['tiempo_bajada_2'] if row['tiempo_bajada_2'] != '-' else self.null_date
        row['tiempo_subida_3'] = row['tiempo_subida_3'] if row['tiempo_subida_3'] != '-' else self.null_date
        row['tiempo_bajada_3'] = row['tiempo_bajada_3'] if row['tiempo_bajada_3'] != '-' else self.null_date
        row['tiempo_subida_4'] = row['tiempo_subida_4'] if row['tiempo_subida_4'] != '-' else self.null_date
        row['tiempo_bajada_4'] = row['tiempo_bajada_4'] if row['tiempo_bajada_4'] != '-' else self.null_date
        row['zona_subida_1'] = int(row['zona_subida_1']) if row['zona_subida_1'].isdigit() else -1
        row['zona_bajada_1'] = int(row['zona_bajada_1']) if row['zona_bajada_1'].isdigit() else -1
        row['zona_subida_2'] = int(row['zona_subida_2']) if row['zona_subida_2'].isdigit() else -1
        row['zona_bajada_2'] = int(row['zona_bajada_2']) if row['zona_bajada_2'].isdigit() else -1
        row['zona_subida_3'] = int(row['zona_subida_3']) if row['zona_subida_3'].isdigit() else -1
        row['zona_bajada_3'] = int(row['zona_bajada_3']) if row['zona_bajada_3'].isdigit() else -1
        row['zona_subida_4'] = int(row['zona_subida_4']) if row['zona_subida_4'].isdigit() else -1
        row['zona_bajada_4'] = int(row['zona_bajada_4']) if row['zona_bajada_4'].isdigit() else -1
        row['mediahora_bajada_1'] = int(row['mediahora_bajada_1']) if row['mediahora_bajada_1'].isdigit() else -1
        row['mediahora_bajada_2'] = int(row['mediahora_bajada_2']) if row['mediahora_bajada_2'].isdigit() else -1
        row['mediahora_bajada_3'] = int(row['mediahora_bajada_3']) if row['mediahora_bajada_3'].isdigit() else -1
        row['mediahora_bajada_4'] = int(row['mediahora_bajada_4']) if row['mediahora_bajada_4'].isdigit() else -1
        row['periodo_bajada_1'] = int(row['periodo_bajada_1']) if row['periodo_bajada_1'].isdigit() else -1
        row['periodo_bajada_2'] = int(row['periodo_bajada_2']) if row['periodo_bajada_2'].isdigit() else -1
        row['periodo_bajada_3'] = int(row['periodo_bajada_3']) if row['periodo_bajada_3'].isdigit() else -1
        row['periodo_bajada_4'] = int(row['periodo_bajada_4']) if row['periodo_bajada_4'].isdigit() else -1

        row.pop(None)
        return row
