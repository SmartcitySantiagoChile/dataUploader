from datauploader.errors import StopDocumentExist
from datauploader.uploader.datafile import DataFile


class StopFile(DataFile):
    """ Class that represents a stop file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['authRouteCode', 'userRouteCode', 'operator', 'order', 'authStopCode', 'userStopCode',
                           'stopName', 'latitude', 'longitude']
        self.uploaded_stops = []

    def row_parser(self, row, path, timestamp):
        if row['userStopCode'] in self.uploaded_stops or row['userStopCode'] == '-':
            raise StopDocumentExist('Stop {0} exists'.format(row['userStopCode']))

        self.uploaded_stops.append(row['userStopCode'])

        return {
            'path': path,
            'timestamp': timestamp,
            'startDate': self.name_to_date(),
            'authCode': row['authStopCode'],
            'userCode': row['userStopCode'],
            'name': row['stopName'],
            'latitude': float(row['latitude']),
            'longitude': float(row['longitude'])
        }
