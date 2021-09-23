from datauploader.uploader.datafile import DataFile


class PaymentFactorFile(DataFile):
    """ Class that represents a payment factor file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["FECHA", "TIPODIA", "ASIGNACION", "ZP", "NOMBRE", "UN", "TOTAL", "SUMAN", "RESTAN",
                           "NEUTRAS", "FACTOR", "SERVS", "TRXS", "VALIDATORID"]

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "date": row['FECHA'],
            "dayType": int(row['TIPODIA']),
            "assignation": row['ASIGNACION'],
            "busStationId": row['ZP'],
            "busStationName": row['NOMBRE'],
            "operator": int(row['UN']),
            "total": float(row['TOTAL']),
            "sum": float(row['SUMAN']),
            "subtraction": float(row['RESTAN']),
            "neutral": float(row['NEUTRAS']),
            "factor": float(row['FACTOR']),
            "routes": row['SERVS'],
            "transactions": row['TRXS'],
            "validatorId": row['VALIDATORID']
        }
