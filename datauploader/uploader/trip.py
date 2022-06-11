from datauploader.uploader.datafile import DataFile


class TripFile(DataFile):
    """ Class that represents a trip file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["tipodia", "factor_expansion", "n_etapas", "tviaje", "distancia_eucl", "distancia_ruta",
                           "tiempo_subida", "tiempo_bajada",
                           "mediahora_subida", "mediahora_bajada",
                           "periodo_subida", "periodo_bajada",
                           "tipo_transporte_1", "tipo_transporte_2", "tipo_transporte_3", "tipo_transporte_4",
                           "srv_1", "srv_2", "srv_3", "srv_4",
                           "paradero_subida", "paradero_bajada", "comuna_subida", "comuna_bajada",
                           "zona_subida", "zona_bajada", "modos",
                           "tiempo_subida1", "tiempo_subida2", "tiempo_subida3", "tiempo_subida4",
                           "tiempo_bajada1", "tiempo_bajada2", "tiempo_bajada3", "tiempo_bajada4",
                           "zona_subida1", "zona_subida2", "zona_subida3", "zona_subida4",
                           "zona_bajada1", "zona_bajada2", "zona_bajada3", "zona_bajada4",
                           "paraderosubida_1era", "paraderosubida_2da", "paraderosubida_3era", "paraderosubida_4ta",
                           "paraderobajada_1era", "paraderobajada_2da", "paraderobajada_3era", "paraderobajada_4ta",
                           "mediahora_bajada_1era", "mediahora_bajada_2da", "mediahora_bajada_3era",
                           "mediahora_bajada_4ta",
                           "periodo_bajada_1era", "periodo_bajada_2da", "periodo_bajada_3era", "periodo_bajada_4ta"]
        self.null_date = "1970-01-01 00:00:00"

    def row_parser(self, row, path, timestamp):
        es_row = dict()
        es_row["path"] = path
        es_row["timestamp"] = timestamp

        es_row['tipodia'] = int(row['tipodia'])
        es_row['factor_expansion'] = float(row['factor_expansion'])
        es_row['n_etapas'] = int(row['n_etapas'])
        es_row['tviaje'] = float(row['tviaje'])
        es_row['distancia_eucl'] = float(row['distancia_eucl'])
        es_row['distancia_ruta'] = float(row['distancia_ruta'])
        es_row['tiempo_subida'] = row['tiempo_subida']
        es_row['tiempo_bajada'] = row['tiempo_bajada']
        es_row['mediahora_subida'] = int(row['mediahora_subida'])
        es_row['mediahora_bajada'] = int(row['mediahora_bajada'])
        es_row['periodo_subida'] = int(row['periodo_subida'])
        es_row['periodo_bajada'] = int(row['periodo_bajada'])
        es_row['tipo_transporte_1'] = int(row['tipo_transporte_1'])
        es_row['tipo_transporte_2'] = int(row['tipo_transporte_2'])
        es_row['tipo_transporte_3'] = int(row['tipo_transporte_3'])
        es_row['tipo_transporte_4'] = int(row['tipo_transporte_4'])
        # si el modo de subida es "Bus + Metro" usar "Bus" como modo de subida
        es_row['modo_subida'] = row['tipo_transporte_1'] if row['tipo_transporte_1'] != 3 else 1
        if es_row['n_etapas'] == 1:
            es_row['servicio_bajada'] = row['srv_1']
            es_row['modo_bajada'] = row['tipo_transporte_1']
        elif es_row['n_etapas'] == 2:
            es_row['servicio_bajada'] = row['srv_2']
            es_row['modo_bajada'] = row['tipo_transporte_2']
        elif es_row['n_etapas'] == 3:
            es_row['servicio_bajada'] = row['srv_3']
            es_row['modo_bajada'] = row['tipo_transporte_3']
        elif es_row['n_etapas'] == 4:
            es_row['servicio_bajada'] = row['srv_4']
            es_row['modo_bajada'] = row['tipo_transporte_4']
        else:
            es_row['modo_bajada'] = 0  # unknown in case of n_etapas > 4
        # si el modo de bajada es "Bus + Metro" usar "Bus" como modo de subida
        es_row['modo_bajada'] = es_row['modo_bajada'] if es_row['modo_bajada'] != 3 else 1
        es_row['srv_1'] = row['srv_1']
        es_row['srv_2'] = row['srv_2']
        es_row['srv_3'] = row['srv_3']
        es_row['srv_4'] = row['srv_4']
        es_row['paradero_subida'] = row['paradero_subida']
        es_row['paradero_bajada'] = row['paradero_bajada']
        es_row['comuna_subida'] = int(row['comuna_subida'])
        es_row['comuna_bajada'] = int(row['comuna_bajada'])
        es_row['zona_subida'] = int(row['zona_subida'])
        es_row['zona_bajada'] = int(row['zona_bajada'])
        es_row['modos'] = int(row['modos'])
        es_row['tiempo_subida_1'] = row['tiempo_subida1'] if row['tiempo_subida1'] != '-' else self.null_date
        es_row['tiempo_bajada_1'] = row['tiempo_bajada1'] if row['tiempo_bajada1'] != '-' else self.null_date
        es_row['tiempo_subida_2'] = row['tiempo_subida2'] if row['tiempo_subida2'] != '-' else self.null_date
        es_row['tiempo_bajada_2'] = row['tiempo_bajada2'] if row['tiempo_bajada2'] != '-' else self.null_date
        es_row['tiempo_subida_3'] = row['tiempo_subida3'] if row['tiempo_subida3'] != '-' else self.null_date
        es_row['tiempo_bajada_3'] = row['tiempo_bajada3'] if row['tiempo_bajada3'] != '-' else self.null_date
        es_row['tiempo_subida_4'] = row['tiempo_subida4'] if row['tiempo_subida4'] != '-' else self.null_date
        es_row['tiempo_bajada_4'] = row['tiempo_bajada4'] if row['tiempo_bajada4'] != '-' else self.null_date
        es_row['zona_subida_1'] = int(row['zona_subida1']) if row['zona_subida1'].isdigit() else -1
        es_row['zona_bajada_1'] = int(row['zona_bajada1']) if row['zona_bajada1'].isdigit() else -1
        es_row['zona_subida_2'] = int(row['zona_subida2']) if row['zona_subida2'].isdigit() else -1
        es_row['zona_bajada_2'] = int(row['zona_bajada2']) if row['zona_bajada2'].isdigit() else -1
        es_row['zona_subida_3'] = int(row['zona_subida3']) if row['zona_subida3'].isdigit() else -1
        es_row['zona_bajada_3'] = int(row['zona_bajada3']) if row['zona_bajada3'].isdigit() else -1
        es_row['zona_subida_4'] = int(row['zona_subida4']) if row['zona_subida4'].isdigit() else -1
        es_row['zona_bajada_4'] = int(row['zona_bajada4']) if row['zona_bajada4'].isdigit() else -1
        es_row['parada_subida_1'] = row['paraderosubida_1era']
        es_row['parada_subida_2'] = row['paraderosubida_2da']
        es_row['parada_subida_3'] = row['paraderosubida_3era']
        es_row['parada_subida_4'] = row['paraderosubida_4ta']
        es_row['parada_bajada_1'] = row['paraderobajada_1era']
        es_row['parada_bajada_2'] = row['paraderobajada_2da']
        es_row['parada_bajada_3'] = row['paraderobajada_3era']
        es_row['parada_bajada_4'] = row['paraderobajada_4ta']
        es_row['mediahora_bajada_1'] = int(row['mediahora_bajada_1era']) if row[
            'mediahora_bajada_1era'].isdigit() else -1
        es_row['mediahora_bajada_2'] = int(row['mediahora_bajada_2da']) if row['mediahora_bajada_2da'].isdigit() else -1
        es_row['mediahora_bajada_3'] = int(row['mediahora_bajada_3era']) if row[
            'mediahora_bajada_3era'].isdigit() else -1
        es_row['mediahora_bajada_4'] = int(row['mediahora_bajada_4ta']) if row['mediahora_bajada_4ta'].isdigit() else -1
        es_row['periodo_bajada_1'] = int(row['periodo_bajada_1era']) if row['periodo_bajada_1era'].isdigit() else -1
        es_row['periodo_bajada_2'] = int(row['periodo_bajada_2da']) if row['periodo_bajada_2da'].isdigit() else -1
        es_row['periodo_bajada_3'] = int(row['periodo_bajada_3era']) if row['periodo_bajada_3era'].isdigit() else -1
        es_row['periodo_bajada_4'] = int(row['periodo_bajada_4ta']) if row['periodo_bajada_4ta'].isdigit() else -1

        return es_row
