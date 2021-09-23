from datauploader.uploader.datafile import DataFile


class SpeedFile(DataFile):
    """ Class that represents a speed file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['Route', 'IdSection', 'Date', 'Period', 'DayType', 'TotalDistance',
                           'TotalTime', 'Speed', 'Observations', 'InvalidObservations', 'Operator', 'RouteUser',
                           'IsEndSection']

    def row_parser(self, row, path, timestamp):
        merged = str(row['Route'] + '-' + row['IdSection'] + '-' + row['Period'])
        return {
            "path": path,
            "timestamp": timestamp,
            "merged": merged,
            "authRouteCode": row['Route'],
            "userRouteCode": row['RouteUser'],
            "operator": int(row['Operator']),
            "section": int(row['IdSection']),
            "date": row['Date'],
            "periodId": int(row['Period']),
            "dayType": int(row['DayType']),
            "totalDistance": float(row['TotalDistance']),
            "totalTime": float(row['TotalTime']),
            "speed": float(row['Speed']),
            "nObs": int(row['Observations']),
            "nInvalidObs": int(row['InvalidObservations']),
            "isEndSection": int(row['IsEndSection'])
        }
