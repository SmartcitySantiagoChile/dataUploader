from datauploader.uploader.datafile import DataFile


class TripFile(DataFile):
    """ Class that represents a trip file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["tipodia", "factor_expansion", "n_etapas", "tviaje", "distancia_eucl", "distancia_ruta",
                           "tiempo_subida",
                           "tiempo_bajada", "mediahora_subida", "mediahora_bajada", "periodo_subida", "periodo_bajada",
                           "tipo_transporte_1", "tipo_transporte_2", "tipo_transporte_3", "tipo_transporte_4", "srv_1",
                           "srv_2", "srv_3",
                           "srv_4", "paradero_subida", "paradero_bajada", "comuna_subida", "comuna_bajada",
                           "zona_subida", "zona_bajada",
                           "modos", "tiempo_subida1", "tiempo_subida2", "tiempo_subida3", "tiempo_subida4",
                           "tiempo_bajada1",
                           "tiempo_bajada2", "tiempo_bajada3", "tiempo_bajada4", "zona_subida1", "zona_bajada1",
                           "zona_subida2",
                           "zona_bajada2", "zona_subida3", "zona_bajada3", "zona_subida4", "zona_bajada4",
                           "paraderosubida_1era",
                           "paraderosubida_2da", "paraderosubida_3era", "paraderosubida_4ta", "paraderobajada_1era",
                           "paraderobajada_2da",
                           "paraderobajada_3era", "paraderobajada_4ta", "mediahora_bajada_1era", "mediahora_bajada_2da",
                           "mediahora_bajada_3era", "mediahora_bajada_4ta", "periodo_bajada_1era", "periodo_bajada_2da",
                           "periodo_bajada_3era", "periodo_bajada_4ta", ""]
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
        row['modo_subida'] = row['tipo_transporte_1'] if row['tipo_transporte_1'] != 3 else 1
        if row['n_etapas'] == 1:
            row['servicio_bajada'] = row['srv_1']
            row['modo_bajada'] = row['tipo_transporte_1']
        elif row['n_etapas'] == 2:
            row['servicio_bajada'] = row['srv_2']
            row['modo_bajada'] = row['tipo_transporte_2']
        elif row['n_etapas'] == 3:
            row['servicio_bajada'] = row['srv_3']
            row['modo_bajada'] = row['tipo_transporte_3']
        elif row['n_etapas'] == 4:
            row['servicio_bajada'] = row['srv_4']
            row['modo_bajada'] = row['tipo_transporte_4']
        else:
            row['modo_bajada'] = 0  # unknown in case of n_etapas > 4
        row['modo_bajada'] = row['modo_bajada'] if row['modo_bajada'] != 3 else 1
        row['comuna_subida'] = int(row['comuna_subida'])
        row['comuna_bajada'] = int(row['comuna_bajada'])
        row['zona_subida'] = int(row['zona_subida'])
        row['zona_bajada'] = int(row['zona_bajada'])
        row['modos'] = int(row['modos'])
        row['tiempo_subida_1'] = row['tiempo_subida1'] if row['tiempo_subida1'] != '-' else self.null_date
        row['tiempo_bajada_1'] = row['tiempo_bajada1'] if row['tiempo_bajada1'] != '-' else self.null_date
        row['tiempo_subida_2'] = row['tiempo_subida2'] if row['tiempo_subida2'] != '-' else self.null_date
        row['tiempo_bajada_2'] = row['tiempo_bajada2'] if row['tiempo_bajada2'] != '-' else self.null_date
        row['tiempo_subida_3'] = row['tiempo_subida3'] if row['tiempo_subida3'] != '-' else self.null_date
        row['tiempo_bajada_3'] = row['tiempo_bajada3'] if row['tiempo_bajada3'] != '-' else self.null_date
        row['tiempo_subida_4'] = row['tiempo_subida4'] if row['tiempo_subida4'] != '-' else self.null_date
        row['tiempo_bajada_4'] = row['tiempo_bajada4'] if row['tiempo_bajada4'] != '-' else self.null_date
        row['zona_subida_1'] = int(row['zona_subida1']) if row['zona_subida1'].isdigit() else -1
        row['zona_bajada_1'] = int(row['zona_bajada1']) if row['zona_bajada1'].isdigit() else -1
        row['zona_subida_2'] = int(row['zona_subida2']) if row['zona_subida2'].isdigit() else -1
        row['zona_bajada_2'] = int(row['zona_bajada2']) if row['zona_bajada2'].isdigit() else -1
        row['zona_subida_3'] = int(row['zona_subida3']) if row['zona_subida3'].isdigit() else -1
        row['zona_bajada_3'] = int(row['zona_bajada3']) if row['zona_bajada3'].isdigit() else -1
        row['zona_subida_4'] = int(row['zona_subida4']) if row['zona_subida4'].isdigit() else -1
        row['zona_bajada_4'] = int(row['zona_bajada4']) if row['zona_bajada4'].isdigit() else -1
        row['mediahora_bajada_1'] = int(row['mediahora_bajada_1era']) if row['mediahora_bajada_1era'].isdigit() else -1
        row['mediahora_bajada_2'] = int(row['mediahora_bajada_2da']) if row['mediahora_bajada_2da'].isdigit() else -1
        row['mediahora_bajada_3'] = int(row['mediahora_bajada_3era']) if row['mediahora_bajada_3era'].isdigit() else -1
        row['mediahora_bajada_4'] = int(row['mediahora_bajada_4ta']) if row['mediahora_bajada_4ta'].isdigit() else -1
        row['periodo_bajada_1'] = int(row['periodo_bajada_1era']) if row['periodo_bajada_1era'].isdigit() else -1
        row['periodo_bajada_2'] = int(row['periodo_bajada_2da']) if row['periodo_bajada_2da'].isdigit() else -1
        row['periodo_bajada_3'] = int(row['periodo_bajada_3era']) if row['periodo_bajada_3era'].isdigit() else -1
        row['periodo_bajada_4'] = int(row['periodo_bajada_4ta']) if row['periodo_bajada_4ta'].isdigit() else -1

        if row.get(None):
            row.pop(None)
        return row
