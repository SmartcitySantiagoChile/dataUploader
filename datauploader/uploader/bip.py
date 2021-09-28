from datauploader.uploader.datafile import DataFile


class BipFile(DataFile):
    """ Class that represents a bip file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["id", "tiempo", "sitio", "op", "servicio_sonda", "servicio_usuario", "periodo", "tipo_dia"]

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "operator": int(row['op']),
            "route": row['servicio_sonda'],
            "userRoute": row['servicio_usuario'],
            "validationTime": row['tiempo'],
            "source": row['sitio'],
            "bipNumber": row['id']
        }
