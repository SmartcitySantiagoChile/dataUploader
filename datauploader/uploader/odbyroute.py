from datauploader.uploader.datafile import DataFile


class OdByRouteFile(DataFile):
    """ Class that represents an odbyroute file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["Dia", "TipoDia", "Servicio", "Operador", "ServicioUsuario", "PeriodoTS", "inicio", "fin",
                           "CodigoParInicio", "CodigoParFin", "UsuarioParInicio", "UsuarioParFin", "NombreParInicio",
                           "NombreParFin", "zonaS", "zonaB", "ConBajada", "SinBajada", "Expandida"]

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "date": row['Dia'],
            "dayType": int(row['TipoDia']),
            "authRouteCode": row['Servicio'],
            "operator": int(row['Operador']),
            "userRouteCode": row['ServicioUsuario'],
            "timePeriodInStopTime": int(row['PeriodoTS']),
            "startStopOrder": int(row['inicio']),
            "endStopOrder": int(row['fin']),
            "authStartStopCode": row['CodigoParInicio'],
            "authEndStopCode": row['CodigoParFin'],
            "userStartStopCode": row['UsuarioParInicio'],
            "userEndStopCode": row['UsuarioParFin'],
            "startStopName": row['NombreParInicio'],
            "endStopName": row['NombreParFin'],
            "startZone": int(row['zonaS']),
            "endZone": int(row['zonaB']),
            "tripNumber": int(row['ConBajada']),
            "tripWithoutLanding": int(row['SinBajada']),
            "expandedTripNumber": float(row['Expandida'])
        }
