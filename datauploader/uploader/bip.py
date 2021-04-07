from datauploader.uploader.datafile import DataFile


class BipFile(DataFile):
    """ Class that represents a bip file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['bipNumber', 'validationTime', 'source', 'operator', 'route', 'userRoute']

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "operator": int(row['operator']),
            "route": row['route'],
            "userRoute": row['userRoute'],
            "validationTime": row['validationTime'],
            "source": row['source'],
            "bipNumber": row['bipNumber']
        }
