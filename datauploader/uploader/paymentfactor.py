from datauploader.uploader.datafile import DataFile


class PaymentFactorFile(DataFile):
    """ Class that represents a payment factor file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ["FECHA", "TIPODIA", "ASIGNACION", "ZP", "NOMBRE", "UN", "TOTAL", "SUMAN", "RESTAN",
                           "NEUTRAS", "FACTOR", "SERVS", "TRXS", "PATENTE", "TRXS_UN", "FACTOR_SIN_TRIO"]

    def row_parser(self, row, path, timestamp):
        operator_transactions = int(row['TRXS_UN']) if row['TRXS_UN'] is not None and row['TRXS_UN'] != '-' else -1
        factor_without_threesome_methodology = float(row['FACTOR_SIN_TRIO']) if \
            row['FACTOR_SIN_TRIO'] is not None and row['FACTOR_SIN_TRIO'] != '-' else -1

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
            "validatorId": row['PATENTE'],
            "operatorTransactions": operator_transactions,
            "factorWithoutThreesomeMethodology": factor_without_threesome_methodology
        }
