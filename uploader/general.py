from uploader.datafile import DataFile


class GeneralFile(DataFile):
    """ Class that represents a general file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['date', 'dayType', 'expeditionNumber', 'minExpeditionTime', 'maxExpeditionTime',
                           'averageExpeditionTime', 'licensePlateNumber', 'GPSPointsNumber',
                           'averageTimeBetweenGPSPoints', 'GPSNumberWithRoute', 'GPSNumberWithoutRoute',
                           'transactionNumber', 'transactionOnBusNumber', 'transactionOnMetroNumber',
                           'transactionOnTrainNumber', 'transactionOnBusStation', 'smartcardNumber',
                           'transactionWithRoute', 'transactionWithoutRoute', 'stagesWithBusAlighting',
                           'stagesWithMetroAlighting', 'stagesWithTrainAlighting', 'stagesWithBusStationAlighting',
                           'tripNumber', 'tripsWithOneStage', 'tripsWithTwoStages', 'tripsWithThreeStages',
                           'tripsWithFourStages', 'tripsWithFiveOrMoreStages', 'tripsWithOnlyMetro',
                           'tripsThatUseMetro', 'tripsWithoutLastAlighting', 'averageTimeOfTrips',
                           'averageDistanceOfTrips', 'averageVelocityOfTrips', 'tripNumberInMorningRushHour',
                           'averageTimeInMorningRushTrips', 'averageDistanceInMorningRushTrips',
                           'averageVelocityInMorningRushTrips', 'tripNumberInAfternoonRushHour',
                           'averageTimeInAfternoonRushTrips', 'averageDistanceInAfternoonRushTrips',
                           'averageVelocityInAfternoonRushTrips']

    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "date": row['date'],
            "dayType": row['dayType'],
            "expeditionNumber": int(row['expeditionNumber']),
            "minExpeditionTime": float(row['minExpeditionTime']),
            "maxExpeditionTime": float(row['maxExpeditionTime']),
            "averageExpeditionTime": float(row['averageExpeditionTime']),
            "licensePlateNumber": int(row['licensePlateNumber']),
            "GPSPointsNumber": int(row['GPSPointsNumber']),
            "averageTimeBetweenGPSPoints": float(row['averageTimeBetweenGPSPoints']),
            "GPSNumberWithRoute": int(row['GPSNumberWithRoute']),
            "GPSNumberWithoutRoute": int(row['GPSNumberWithoutRoute']),
            "transactionNumber": int(row['transactionNumber']),
            "transactionOnBusNumber": float(row['transactionOnBusNumber']),
            "transactionOnMetroNumber": float(row['transactionOnMetroNumber']),
            "transactionOnTrainNumber": float(row['transactionOnTrainNumber']),
            "transactionOnBusStation": float(row['transactionOnBusStation']),
            "smartcardNumber": int(row['smartcardNumber']),
            "transactionWithRoute": int(row['transactionWithRoute']),
            "transactionWithoutRoute": int(row['transactionWithoutRoute']),
            "stagesWithBusAlighting": float(row['stagesWithBusAlighting']),
            "stagesWithMetroAlighting": float(row['stagesWithMetroAlighting']),
            "stagesWithTrainAlighting": float(row['stagesWithTrainAlighting']),
            "stagesWithBusStationAlighting": float(row['stagesWithBusStationAlighting']),
            "tripNumber": int(row['tripNumber']),
            "tripsWithOneStage": float(row['tripsWithOneStage']),
            "tripsWithTwoStages": float(row['tripsWithTwoStages']),
            "tripsWithThreeStages": float(row['tripsWithThreeStages']),
            "tripsWithFourStages": float(row['tripsWithFourStages']),
            "tripsWithFiveOrMoreStages": float(row['tripsWithFiveOrMoreStages']),
            "tripsWithOnlyMetro": float(row['tripsWithOnlyMetro']),
            "tripsThatUseMetro": float(row['tripsThatUseMetro']),
            "tripsWithoutLastAlighting": float(row['tripsWithoutLastAlighting']),
            "averageTimeOfTrips": float(row['averageTimeOfTrips']),
            "averageDistanceOfTrips": float(row['averageDistanceOfTrips']),
            "averageVelocityOfTrips": float(row['averageVelocityOfTrips']),
            "tripNumberInMorningRushHour": float(row['tripNumberInMorningRushHour']),
            "averageTimeInMorningRushTrips": float(row['averageTimeInMorningRushTrips']),
            "averageDistanceInMorningRushTrips": float(row['averageDistanceInMorningRushTrips']),
            "averageVelocityInMorningRushTrips": float(row['averageVelocityInMorningRushTrips']),
            "tripNumberInAfternoonRushHour": float(row['tripNumberInAfternoonRushHour']),
            "averageTimeInAfternoonRushTrips": float(row['averageTimeInAfternoonRushTrips']),
            "averageDistanceInAfternoonRushTrips": float(row['averageDistanceInAfternoonRushTrips']),
            "averageVelocityInAfternoonRushTrips": float(row['averageVelocityInAfternoonRushTrips'])
        }
