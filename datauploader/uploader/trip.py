import os
from collections import OrderedDict
from datetime import datetime

from datauploader.errors import FilterDocumentError
from datauploader.uploader.datafile import DataFile


class TripFile(DataFile):
    """Class that represents a trip file."""

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        # Set fieldnames by date range
        fieldnames_date_range = OrderedDict()
        fieldnames_date_range["1970-01-01"] = [
            "tipodia",
            "factor_expansion",
            "n_etapas",
            "tviaje",
            "distancia_eucl",
            "distancia_ruta",
            "tiempo_subida",
            "tiempo_bajada",
            "mediahora_subida",
            "mediahora_bajada",
            "periodo_subida",
            "periodo_bajada",
            "tipo_transporte_1",
            "tipo_transporte_2",
            "tipo_transporte_3",
            "tipo_transporte_4",
            "srv_1",
            "srv_2",
            "srv_3",
            "srv_4",
            "paradero_subida",
            "paradero_bajada",
            "comuna_subida",
            "comuna_bajada",
            "zona_subida",
            "zona_bajada",
            "modos",
            "tiempo_subida1",
            "tiempo_subida2",
            "tiempo_subida3",
            "tiempo_subida4",
            "tiempo_bajada1",
            "tiempo_bajada2",
            "tiempo_bajada3",
            "tiempo_bajada4",
            "zona_subida1",
            "zona_subida2",
            "zona_subida3",
            "zona_subida4",
            "zona_bajada1",
            "zona_bajada2",
            "zona_bajada3",
            "zona_bajada4",
            "paraderosubida_1era",
            "paraderosubida_2da",
            "paraderosubida_3era",
            "paraderosubida_4ta",
            "paraderobajada_1era",
            "paraderobajada_2da",
            "paraderobajada_3era",
            "paraderobajada_4ta",
            "mediahora_bajada_1era",
            "mediahora_bajada_2da",
            "mediahora_bajada_3era",
            "mediahora_bajada_4ta",
            "periodo_bajada_1era",
            "periodo_bajada_2da",
            "periodo_bajada_3era",
            "periodo_bajada_4ta",
        ]
        fieldnames_date_range["2022-10-01"] = [
            "tipodia",
            "factor_expansion",
            "n_etapas",
            "tviaje",
            "distancia_eucl",
            "distancia_ruta",
            "tiempo_inicio_viaje",
            "tiempo_fin_viaje",
            "mediahora_inicio_viaje",
            "mediahora_fin_viaje",
            "periodo_inicio_viaje",
            "periodo_fin_viaje",
            "tipo_transporte_1",
            "tipo_transporte_2",
            "tipo_transporte_3",
            "tipo_transporte_4",
            "srv_1",
            "srv_2",
            "srv_3",
            "srv_4",
            "paradero_inicio_viaje",
            "paradero_fin_viaje",
            "comuna_inicio_viaje",
            "comuna_fin_viaje",
            "zona_inicio_viaje",
            "zona_fin_viaje",
            "modos",
            "tiempo_subida_1",
            "tiempo_subida_2",
            "tiempo_subida_3",
            "tiempo_subida_4",
            "tiempo_bajada_1",
            "tiempo_bajada_2",
            "tiempo_bajada_3",
            "tiempo_bajada_4",
            "zona_subida_1",
            "zona_subida_2",
            "zona_subida_3",
            "zona_subida_4",
            "zona_bajada_1",
            "zona_bajada_2",
            "zona_bajada_3",
            "zona_bajada_4",
            "paradero_subida_1",
            "paradero_subida_2",
            "paradero_subida_3",
            "paradero_subida_4",
            "paradero_bajada_1",
            "paradero_bajada_2",
            "paradero_bajada_3",
            "paradero_bajada_4",
            "mediahora_bajada_1",
            "mediahora_bajada_2",
            "mediahora_bajada_3",
            "mediahora_bajada_4",
            "periodo_bajada_1",
            "periodo_bajada_2",
            "periodo_bajada_3",
            "periodo_bajada_4",
            "id_tarjeta",
            "id_viaje",
            "netapassinbajada",
            "ultimaetapaconbajada",
            "contrato",
            "mediahora_inicio_viaje_hora",
            "mediahora_fin_viaje_hora",
            "op_1era_etapa",
            "op_2da_etapa",
            "op_3era_etapa",
            "op_4ta_etapa",
            "dt1",
            "dveh_ruta1",
            "dveh_euc1",
            "dt2",
            "dveh_ruta2",
            "dveh_euc2",
            "dt3",
            "dveh_ruta3",
            "dveh_euc3",
            "dveh_ruta4",
            "dveh_euc4",
            "dtfinal",
            "dveh_rutafinal",
            "dveh_eucfinal",
            "tipo_corte_etapa_viaje",
            "proposito",
            "entrada",
            "te0",
            "tv1",
            "tc1",
            "te1",
            "tv2",
            "tc2",
            "te2",
            "tv3",
            "tc3",
            "te3",
            "tv4",
            "egreso",
            "tviaje2",
        ]

        file_date = os.path.basename(datafile).split(".")[0]
        formated_date = datetime.strptime(file_date, "%Y-%m-%d")
        self.fieldnames = []
        for key, value in fieldnames_date_range.items():
            if formated_date >= datetime.strptime(key, "%Y-%m-%d"):
                self.fieldnames = value
        self.null_date = "1970-01-01 00:00:00"

    def row_parser(self, row, path, timestamp):
        es_row = dict()
        es_row["path"] = path
        es_row["timestamp"] = timestamp

        # New columns post 2022-09-30
        # Case 1: similar names
        if row.get("tiempo_inicio_viaje"):
            row["tiempo_subida"] = row["tiempo_inicio_viaje"]
        if row.get("tiempo_fin_viaje"):
            row["tiempo_bajada"] = row["tiempo_fin_viaje"]
        if row.get("mediahora_inicio_viaje"):
            row["mediahora_subida"] = row["mediahora_inicio_viaje"]
        if row.get("mediahora_fin_viaje"):
            row["mediahora_bajada"] = row["mediahora_fin_viaje"]
        if row.get("periodo_inicio_viaje"):
            row["periodo_subida"] = row["periodo_inicio_viaje"]
        if row.get("periodo_fin_viaje"):
            row["periodo_bajada"] = row["periodo_fin_viaje"]
        if row.get("paradero_inicio_viaje"):
            row["paradero_subida"] = row["paradero_inicio_viaje"]
        if row.get("paradero_fin_viaje"):
            row["paradero_bajada"] = row["paradero_fin_viaje"]
        if row.get("comuna_inicio_viaje"):
            row["comuna_subida"] = row["comuna_inicio_viaje"]
        if row.get("comuna_fin_viaje"):
            row["comuna_bajada"] = row["comuna_fin_viaje"]
        if row.get("zona_inicio_viaje"):
            row["zona_subida"] = row["zona_inicio_viaje"]
        if row.get("zona_fin_viaje"):
            row["zona_bajada"] = row["zona_fin_viaje"]
        if row.get("tiempo_subida_1"):
            row["tiempo_subida1"] = row["tiempo_subida_1"]
        if row.get("tiempo_subida_2"):
            row["tiempo_subida2"] = row["tiempo_subida_2"]
        if row.get("tiempo_subida_3"):
            row["tiempo_subida3"] = row["tiempo_subida_3"]
        if row.get("tiempo_subida_4"):
            row["tiempo_subida4"] = row["tiempo_subida_4"]
        if row.get("tiempo_bajada_1"):
            row["tiempo_bajada1"] = row["tiempo_bajada_1"]
        if row.get("tiempo_bajada_2"):
            row["tiempo_bajada2"] = row["tiempo_bajada_2"]
        if row.get("tiempo_bajada_3"):
            row["tiempo_bajada3"] = row["tiempo_bajada_3"]
        if row.get("tiempo_bajada_4"):
            row["tiempo_bajada4"] = row["tiempo_bajada_4"]

        if row.get("zona_subida_1"):
            row["zona_subida1"] = row["zona_subida_1"]
        if row.get("zona_subida_2"):
            row["zona_subida2"] = row["zona_subida_2"]
        if row.get("zona_subida_3"):
            row["zona_subida3"] = row["zona_subida_3"]
        if row.get("zona_subida_4"):
            row["zona_subida4"] = row["zona_subida_4"]
        if row.get("zona_bajada_1"):
            row["zona_bajada1"] = row["zona_bajada_1"]
        if row.get("zona_bajada_2"):
            row["zona_bajada2"] = row["zona_bajada_2"]
        if row.get("zona_bajada_3"):
            row["zona_bajada3"] = row["zona_bajada_3"]
        if row.get("zona_bajada_4"):
            row["zona_bajada4"] = row["zona_bajada_4"]

        if row.get("paradero_subida_1"):
            row["paraderosubida_1era"] = row["paradero_subida_1"]
        if row.get("paradero_subida_2"):
            row["paraderosubida_2da"] = row["paradero_subida_2"]
        if row.get("paradero_subida_3"):
            row["paraderosubida_3era"] = row["paradero_subida_3"]
        if row.get("paradero_subida_4"):
            row["paraderosubida_4ta"] = row["paradero_subida_4"]
        if row.get("paradero_bajada_1"):
            row["paraderobajada_1era"] = row["paradero_bajada_1"]
        if row.get("paradero_bajada_2"):
            row["paraderobajada_2da"] = row["paradero_bajada_2"]
        if row.get("paradero_bajada_3"):
            row["paraderobajada_3era"] = row["paradero_bajada_3"]
        if row.get("paradero_bajada_4"):
            row["paraderobajada_4ta"] = row["paradero_bajada_4"]

        if row.get("mediahora_bajada_1"):
            row["mediahora_bajada_1era"] = row["mediahora_bajada_1"]
        if row.get("mediahora_bajada_2"):
            row["mediahora_bajada_2da"] = row["mediahora_bajada_2"]
        if row.get("mediahora_bajada_3"):
            row["mediahora_bajada_3era"] = row["mediahora_bajada_3"]
        if row.get("mediahora_bajada_4"):
            row["mediahora_bajada_4ta"] = row["mediahora_bajada_4"]

        if row.get("periodo_bajada_1"):
            row["periodo_bajada_1era"] = row["periodo_bajada_1"]
        if row.get("periodo_bajada_2"):
            row["periodo_bajada_2da"] = row["periodo_bajada_2"]
        if row.get("periodo_bajada_3"):
            row["periodo_bajada_3era"] = row["periodo_bajada_3"]
        if row.get("periodo_bajada_4"):
            row["periodo_bajada_4ta"] = row["periodo_bajada_4"]

        # Case 2: new columns
        es_row["id_tarjeta"] = self.get_int_value_or_minus_one(row, "id_tarjeta")
        es_row["id_viaje"] = self.get_int_value_or_minus_one(row, "id_viaje")
        es_row["n_etapas_sin_bajada"] = self.get_int_value_or_minus_one(
            row, "netapassinbajada"
        )
        es_row["ultima_etapa_con_bajada"] = self.get_int_value_or_minus_one(
            row, "ultimaetapaconbajada"
        )
        es_row["contrato"] = self.get_int_value_or_minus_one(row, "contrato")
        es_row["mediahora_inicio_viaje_hora"] = (
            row["mediahora_inicio_viaje_hora"]
            if row.get("mediahora_inicio_viaje_hora")
               and row.get("mediahora_inicio_viaje_hora") != "-"
            else "00:00:00"
        )
        es_row["mediahora_fin_viaje_hora"] = (
            row["mediahora_fin_viaje_hora"]
            if row.get("mediahora_fin_viaje_hora")
               and row.get("mediahora_fin_viaje_hora") != "-"
            else "00:00:00"
        )

        es_row["op_etapa_1"] = self.get_int_value_or_minus_one(row, "op_1era_etapa")
        es_row["op_etapa_2"] = self.get_int_value_or_minus_one(row, "op_2da_etapa")
        es_row["op_etapa_3"] = self.get_int_value_or_minus_one(row, "op_3era_etapa")
        es_row["op_etapa_4"] = self.get_int_value_or_minus_one(row, "op_4ta_etapa")
        es_row["distancia_caminata_1"] = self.get_float_value_or_minus_one(row, "dt1")
        es_row["distancia_caminata_2"] = self.get_float_value_or_minus_one(row, "dt2")
        es_row["distancia_caminata_3"] = self.get_float_value_or_minus_one(row, "dt3")
        es_row["distancia_caminata_final"] = self.get_float_value_or_minus_one(
            row, "dtfinal"
        )
        es_row["distancia_vehiculo_ruta_1"] = self.get_int_value_or_minus_one(
            row, "dveh_ruta1"
        )
        es_row["distancia_vehiculo_ruta_2"] = self.get_int_value_or_minus_one(
            row, "dveh_ruta2"
        )
        es_row["distancia_vehiculo_ruta_3"] = self.get_int_value_or_minus_one(
            row, "dveh_ruta3"
        )
        es_row["distancia_vehiculo_ruta_4"] = self.get_int_value_or_minus_one(
            row, "dveh_ruta4"
        )
        es_row["distancia_vehiculo_ruta_final"] = self.get_int_value_or_minus_one(
            row, "dveh_rutafinal"
        )
        es_row[
            "distancia_euclidiana_vehiculo_ruta_1"
        ] = self.get_int_value_or_minus_one(row, "dveh_euc1")
        es_row[
            "distancia_euclidiana_vehiculo_ruta_2"
        ] = self.get_int_value_or_minus_one(row, "dveh_euc2")
        es_row[
            "distancia_euclidiana_vehiculo_ruta_3"
        ] = self.get_int_value_or_minus_one(row, "dveh_euc3")
        es_row[
            "distancia_euclidiana_vehiculo_ruta_4"
        ] = self.get_int_value_or_minus_one(row, "dveh_euc4")
        es_row[
            "distancia_euclidiana_vehiculo_ruta_final"
        ] = self.get_int_value_or_minus_one(row, "dveh_eucfinal")
        es_row["tipo_corte_etapa_viaje"] = self.get_string_value_or_hyphen(
            row, "tipo_corte_etapa_viaje"
        )
        es_row["proposito"] = self.get_string_value_or_hyphen(row, "proposito")
        es_row["tiempo_entrada"] = self.get_int_value_or_minus_one(row, "entrada")
        es_row["tiempo_espera_0"] = self.get_int_value_or_minus_one(row, "te0")
        es_row["tiempo_espera_1"] = self.get_int_value_or_minus_one(row, "te1")
        es_row["tiempo_espera_2"] = self.get_int_value_or_minus_one(row, "te2")
        es_row["tiempo_espera_3"] = self.get_int_value_or_minus_one(row, "te3")
        es_row["tiempo_viaje_vehiculo_1"] = self.get_int_value_or_minus_one(row, "tv1")
        es_row["tiempo_viaje_vehiculo_2"] = self.get_int_value_or_minus_one(row, "tv2")
        es_row["tiempo_viaje_vehiculo_3"] = self.get_int_value_or_minus_one(row, "tv3")
        es_row["tiempo_viaje_vehiculo_4"] = self.get_int_value_or_minus_one(row, "tv4")
        es_row["tiempo_caminata_etapa_1"] = self.get_int_value_or_minus_one(row, "tc1")
        es_row["tiempo_caminata_etapa_2"] = self.get_int_value_or_minus_one(row, "tc2")
        es_row["tiempo_caminata_etapa_3"] = self.get_int_value_or_minus_one(row, "tc3")
        es_row["tiempo_caminata_etapa_4"] = self.get_int_value_or_minus_one(row, "tc4")
        es_row["tiempo_egreso"] = self.get_int_value_or_minus_one(row, "egreso")

        # Tviaje case
        if row.get("tviaje2"):
            es_row["tviaje"] = self.get_float_value_or_minus_one(row, "tviaje2")
        else:
            es_row["tviaje"] = self.get_float_value_or_minus_one(row, "tviaje")

        # Legacy columns
        es_row["tipodia"] = int(row["tipodia"])
        es_row["factor_expansion"] = float(row["factor_expansion"])
        es_row["n_etapas"] = int(row["n_etapas"])
        es_row["distancia_eucl"] = self.get_float_value_or_minus_one(
            row, "distancia_eucl"
        )
        es_row["distancia_ruta"] = float(row["distancia_ruta"])
        es_row["tiempo_subida"] = (
            row["tiempo_subida"] if row["tiempo_subida"] != "-" else self.null_date
        )
        es_row["tiempo_bajada"] = (
            row["tiempo_bajada"] if row["tiempo_bajada"] != "-" else self.null_date
        )
        es_row["mediahora_subida"] = int(row["mediahora_subida"])
        es_row["mediahora_bajada"] = int(row["mediahora_bajada"])
        es_row["periodo_subida"] = int(row["periodo_subida"])
        es_row["periodo_bajada"] = int(row["periodo_bajada"])
        es_row["tipo_transporte_1"] = self.get_int_value_or_minus_one(
            row, "tipo_transporte_1"
        )
        es_row["tipo_transporte_2"] = self.get_int_value_or_minus_one(
            row, "tipo_transporte_2"
        )
        es_row["tipo_transporte_3"] = self.get_int_value_or_minus_one(
            row, "tipo_transporte_3"
        )
        es_row["tipo_transporte_4"] = self.get_int_value_or_minus_one(
            row, "tipo_transporte_4"
        )
        # si el modo de subida es "Bus + Metro" usar "Bus" como modo de subida
        es_row["modo_subida"] = (
            row["tipo_transporte_1"] if row["tipo_transporte_1"] != 3 else 1
        )
        if es_row["n_etapas"] == 1:
            es_row["servicio_bajada"] = row["srv_1"]
            es_row["modo_bajada"] = row["tipo_transporte_1"]
        elif es_row["n_etapas"] == 2:
            es_row["servicio_bajada"] = row["srv_2"]
            es_row["modo_bajada"] = row["tipo_transporte_2"]
        elif es_row["n_etapas"] == 3:
            es_row["servicio_bajada"] = row["srv_3"]
            es_row["modo_bajada"] = row["tipo_transporte_3"]
        elif es_row["n_etapas"] == 4:
            es_row["servicio_bajada"] = row["srv_4"]
            es_row["modo_bajada"] = row["tipo_transporte_4"]
        else:
            es_row["modo_bajada"] = 0  # unknown in case of n_etapas > 4
        # si el modo de bajada es "Bus + Metro" usar "Bus" como modo de subida
        es_row["modo_bajada"] = (
            es_row["modo_bajada"] if es_row["modo_bajada"] != 3 else 1
        )
        es_row["srv_1"] = row["srv_1"]
        es_row["srv_2"] = row["srv_2"]
        es_row["srv_3"] = row["srv_3"]
        es_row["srv_4"] = row["srv_4"]
        es_row["paradero_subida"] = row["paradero_subida"]
        es_row["paradero_bajada"] = row["paradero_bajada"]
        es_row["comuna_subida"] = int(row["comuna_subida"])
        es_row["comuna_bajada"] = int(row["comuna_bajada"])
        es_row["zona_subida"] = self.get_int_value_or_minus_one(row, "zona_subida")
        es_row["zona_bajada"] = self.get_int_value_or_minus_one(row, "zona_bajada")
        es_row["modos"] = int(row["modos"])
        es_row["tiempo_subida_1"] = (
            row["tiempo_subida1"] if row["tiempo_subida1"] != "-" else self.null_date
        )
        es_row["tiempo_bajada_1"] = (
            row["tiempo_bajada1"] if row["tiempo_bajada1"] != "-" else self.null_date
        )
        es_row["tiempo_subida_2"] = (
            row["tiempo_subida2"] if row["tiempo_subida2"] != "-" else self.null_date
        )
        es_row["tiempo_bajada_2"] = (
            row["tiempo_bajada2"] if row["tiempo_bajada2"] != "-" else self.null_date
        )
        es_row["tiempo_subida_3"] = (
            row["tiempo_subida3"] if row["tiempo_subida3"] != "-" else self.null_date
        )
        es_row["tiempo_bajada_3"] = (
            row["tiempo_bajada3"] if row["tiempo_bajada3"] != "-" else self.null_date
        )
        es_row["tiempo_subida_4"] = (
            row["tiempo_subida4"] if row["tiempo_subida4"] != "-" else self.null_date
        )
        es_row["tiempo_bajada_4"] = (
            row["tiempo_bajada4"] if row["tiempo_bajada4"] != "-" else self.null_date
        )
        es_row["zona_subida_1"] = (
            int(row["zona_subida1"]) if row["zona_subida1"].isdigit() else -1
        )
        es_row["zona_bajada_1"] = (
            int(row["zona_bajada1"]) if row["zona_bajada1"].isdigit() else -1
        )
        es_row["zona_subida_2"] = (
            int(row["zona_subida2"]) if row["zona_subida2"].isdigit() else -1
        )
        es_row["zona_bajada_2"] = (
            int(row["zona_bajada2"]) if row["zona_bajada2"].isdigit() else -1
        )
        es_row["zona_subida_3"] = (
            int(row["zona_subida3"]) if row["zona_subida3"].isdigit() else -1
        )
        es_row["zona_bajada_3"] = (
            int(row["zona_bajada3"]) if row["zona_bajada3"].isdigit() else -1
        )
        es_row["zona_subida_4"] = (
            int(row["zona_subida4"]) if row["zona_subida4"].isdigit() else -1
        )
        es_row["zona_bajada_4"] = (
            int(row["zona_bajada4"]) if row["zona_bajada4"].isdigit() else -1
        )
        es_row["parada_subida_1"] = row["paraderosubida_1era"]
        es_row["parada_subida_2"] = row["paraderosubida_2da"]
        es_row["parada_subida_3"] = row["paraderosubida_3era"]
        es_row["parada_subida_4"] = row["paraderosubida_4ta"]
        es_row["parada_bajada_1"] = row["paraderobajada_1era"]
        es_row["parada_bajada_2"] = row["paraderobajada_2da"]
        es_row["parada_bajada_3"] = row["paraderobajada_3era"]
        es_row["parada_bajada_4"] = row["paraderobajada_4ta"]
        es_row["mediahora_bajada_1"] = (
            int(row["mediahora_bajada_1era"])
            if row["mediahora_bajada_1era"].isdigit()
            else -1
        )
        es_row["mediahora_bajada_2"] = (
            int(row["mediahora_bajada_2da"])
            if row["mediahora_bajada_2da"].isdigit()
            else -1
        )
        es_row["mediahora_bajada_3"] = (
            int(row["mediahora_bajada_3era"])
            if row["mediahora_bajada_3era"].isdigit()
            else -1
        )
        es_row["mediahora_bajada_4"] = (
            int(row["mediahora_bajada_4ta"])
            if row["mediahora_bajada_4ta"].isdigit()
            else -1
        )
        es_row["periodo_bajada_1"] = (
            int(row["periodo_bajada_1era"])
            if row["periodo_bajada_1era"].isdigit()
            else -1
        )
        es_row["periodo_bajada_2"] = (
            int(row["periodo_bajada_2da"])
            if row["periodo_bajada_2da"].isdigit()
            else -1
        )
        es_row["periodo_bajada_3"] = (
            int(row["periodo_bajada_3era"])
            if row["periodo_bajada_3era"].isdigit()
            else -1
        )
        es_row["periodo_bajada_4"] = (
            int(row["periodo_bajada_4ta"])
            if row["periodo_bajada_4ta"].isdigit()
            else -1
        )

        try:
            speed_m_s = es_row['distancia_eucl'] / es_row['tviaje']
            speed_km_hr = speed_m_s * 3600 / 1000
        except ZeroDivisionError:
            raise FilterDocumentError

        # check conditions to upload file
        if es_row["paradero_bajada"] != '-' and \
                es_row["factor_expansion"] > 0 and \
                60 * 1 < es_row["tviaje"] < 60 * 60 * 4 and \
                es_row["n_etapas"] <= 5 and \
                es_row["distancia_eucl"] < 50 * 1000 and \
                speed_km_hr <= 120:
            return es_row
        else:
            raise FilterDocumentError
