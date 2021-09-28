from datauploader.uploader.datafile import DataFile


class GeneralFile(DataFile):
    """ Class that represents a general file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['Fecha', 'TipoDia', 'version', 'nExpediciones', 'minTiempoExpediciones',
                           'maxTiempoExpediciones',
                           'mediaTiempoExpediciones', 'nPatentes', 'nGPS',
                           'mediaTiempoEntreGPS', 'nGPSConServicio', 'nGPSSinServicio',
                           'nTrxTotales', 'nTrxTotalesBus(%)', 'nTrxTotalesMetro(%)',
                           'nTrxTotalesMetroTren(%)', 'nTrxTotalesZonasPagas(%)', 'nTarjetas',
                           'nTrxsConServicio', 'nTrxsSinServicio', 'nEtapasConBajadaBus(%)',
                           'nEtapasConBajadaMetro(%)', 'nEtapasConBajadaMetroTren(%)', 'nEtapasConBajadaZonasPagas(%)',
                           'nViajes', 'nViajes1E(%)', 'nViajes2E(%)', 'nViajes3E(%)',
                           'nViajes4E(%)', 'nViajes5E(%)', 'nViajesSoloMetro(%)',
                           'nViajesConAlgunaEtapaEnMetro(%)', 'nViajesSinBajadaFinal(%)', 'tViajeTotal',
                           'dViajeTotal', 'vViajeTotal', 'nViajeMediaPM',
                           'tViajeMediaPM', 'dViajeMediaPM',
                           'vViajeMediaPM', 'nViajeMediaPT',
                           'tViajeMediaPT', 'dViajeMediaPT',
                           'vViajeMediaPT', "nTrxPM(%)",
                           "nTrxPT(%)", "nBajadas", "nBajadasPM",
                           "nBajadasPT", "nParadasE", "nParadasT",
                           "nParadasL", "nParadasI", "nTrxE(%)",
                           "nTrxT(%)", "nTrxL(%)",
                           "nTrxI(%)", "par1",
                           "par2", "par3",
                           "par4", "par5",
                           "par6", "par7",
                           "par8", "par9",
                           "par10", "trx1",
                           "trx2",
                           "trx3",
                           "trx4",
                           "trx5",
                           "trx6",
                           "trx7",
                           "trx8",
                           "trx9",
                           "trx10", "parBus1",
                           "parBus2", "parBus3",
                           "parBus4", "parBus5",
                           "parBus6", "parBus7",
                           "parBus8", "parBus9",
                           "parBus10", "trx1_", "trx2_", "trx3_", "trx4_", "trx5_", "trx6_", "trx7_",
                           "trx8_", "trx9_", "trx10_"]


    def row_parser(self, row, path, timestamp):
        return {
            "path": path,
            "timestamp": timestamp,
            "date": row['Fecha'],
            "dayType": row['TipoDia'],
            "version": row['version'],
            "expeditionNumber": int(row['nExpediciones']),
            "minExpeditionTime": float(row['minTiempoExpediciones']),
            "maxExpeditionTime": float(row['maxTiempoExpediciones']),
            "averageExpeditionTime": float(row['mediaTiempoExpediciones']),
            "licensePlateNumber": int(row['nPatentes']),
            "GPSPointsNumber": int(row['nGPS']),
            "averageTimeBetweenGPSPoints": float(row['mediaTiempoEntreGPS']),
            "GPSNumberWithRoute": int(row['nGPSConServicio']),
            "GPSNumberWithoutRoute": int(row['nGPSSinServicio']),
            "transactionNumber": int(row['nTrxTotales']),
            "transactionOnBusNumber": float(row['nTrxTotalesBus(%)']),
            "transactionOnMetroNumber": float(row['nTrxTotalesMetro(%)']),
            "transactionOnTrainNumber": float(row['nTrxTotalesMetroTren(%)']),
            "transactionOnBusStation": float(row['nTrxTotalesZonasPagas(%)']),
            "smartcardNumber": int(row['nTarjetas']),
            "transactionWithRoute": int(row['nTrxsConServicio']),
            "transactionWithoutRoute": int(row['nTrxsSinServicio']),
            "stagesWithBusAlighting": float(row['nEtapasConBajadaBus(%)']),
            "stagesWithMetroAlighting": float(row['nEtapasConBajadaMetro(%)']),
            "stagesWithTrainAlighting": float(row['nEtapasConBajadaMetroTren(%)']),
            "stagesWithBusStationAlighting": float(row['nEtapasConBajadaZonasPagas(%)']),
            "tripNumber": int(row['nViajes']),
            "tripsWithOneStage": float(row['nViajes1E(%)']),
            "tripsWithTwoStages": float(row['nViajes2E(%)']),
            "tripsWithThreeStages": float(row['nViajes3E(%)']),
            "tripsWithFourStages": float(row['nViajes4E(%)']),
            "tripsWithFiveOrMoreStages": float(row['nViajes5E(%)']),
            "tripsWithOnlyMetro": float(row['nViajesSoloMetro(%)']),
            "tripsThatUseMetro": float(row['nViajesConAlgunaEtapaEnMetro(%)']),
            "tripsWithoutLastAlighting": float(row['nViajesSinBajadaFinal(%)']),
            "averageTimeOfTrips": float(row['tViajeTotal']),
            "averageDistanceOfTrips": float(row['dViajeTotal']),
            "averageVelocityOfTrips": float(row['vViajeTotal']),
            "tripNumberInMorningRushHour": float(row['nViajeMediaPM']),
            "averageTimeInMorningRushTrips": float(row['tViajeMediaPM']),
            "averageDistanceInMorningRushTrips": float(row['dViajeMediaPM']),
            "averageVelocityInMorningRushTrips": float(row['vViajeMediaPM']),
            "tripNumberInAfternoonRushHour": float(row['nViajeMediaPT']),
            "averageTimeInAfternoonRushTrips": float(row['tViajeMediaPT']),
            "averageDistanceInAfternoonRushTrips": float(row['dViajeMediaPT']),
            "averageVelocityInAfternoonRushTrips": float(row['vViajeMediaPT']),
            "transactionInMorningRushHour": float(row['nTrxPM(%)']),
            "transactionInAfternoonRushHour": float(row['nTrxPT(%)']),
            "alightingNumber": float(row['nBajadas']),
            "alightingNumberInMorningRushHour": float(row['nBajadasPM']),
            "alightingNumberInAfternoonRushHour": float(row['nBajadasPT']),
            "stopsNumberWithTypeE": float(row['nParadasE']),
            "stopsNumberWithTypeT": float(row['nParadasT']),
            "stopsNumberWithTypeL": float(row['nParadasL']),
            "stopsNumberWithTypeI": float(row['nParadasI']),
            "transactionNumberInStopsWithTypeE": float(row['nTrxE(%)']),
            "transactionNumberInStopsWithTypeT": float(row['nTrxT(%)']),
            "transactionNumberInStopsWithTypeL": float(row['nTrxL(%)']),
            "transactionNumberInStopsWithTypeI": float(row['nTrxI(%)']),
            "firstStopWithMoreValidations": row['par1'],
            "secondStopWithMoreValidations": row['par2'],
            "thirdStopWithMoreValidations": row['par3'],
            "fourthStopWithMoreValidations": row['par4'],
            "fifthStopWithMoreValidations": row['par5'],
            "sixthStopWithMoreValidations": row['par6'],
            "seventhStopWithMoreValidations": row['par7'],
            "eighthStopWithMoreValidations": row['par8'],
            "ninethStopWithMoreValidations": row['par9'],
            "tenthStopWithMoreValidations": row['par10'],
            "transactionNumberInFirstStopWithMoreValidations": float(
                row['trx1']),
            "transactionNumberInSecondStopWithMoreValidations": float(
                row['trx2']),
            "transactionNumberInThirdStopWithMoreValidations": float(
                row['trx3']),
            "transactionNumberInFourthStopWithMoreValidations": float(
                row['trx4']),
            "transactionNumberInFifthStopWithMoreValidations": float(
                row['trx5']),
            "transactionNumberInSixthStopWithMoreValidations": float(
                row['trx6']),
            "transactionNumberInSeventhStopWithMoreValidations": float(
                row['trx7']),
            "transactionNumberInEighthStopWithMoreValidations": float(
                row['trx8']),
            "transactionNumberInNinethStopWithMoreValidations": float(
                row['trx9']),
            "transactionNumberInTenthStopWithMoreValidations": float(
                row['trx10']),
            "firstBusStopWithMoreValidations": row['parBus1'],
            "secondBusStopWithMoreValidations": row['parBus2'],
            "thirdBusStopWithMoreValidations": row['parBus3'],
            "fourthBusStopWithMoreValidations": row['parBus4'],
            "fifthBusStopWithMoreValidations": row['parBus5'],
            "sixthBusStopWithMoreValidations": row['parBus6'],
            "seventhBusStopWithMoreValidations": row['parBus7'],
            "eighthBusStopWithMoreValidations": row['parBus8'],
            "ninethBusStopWithMoreValidations": row['parBus9'],
            "tenthBusStopWithMoreValidations": row['parBus10'],
            "transactionNumberInFirstBusStopWithMoreValidations": float(
                row['trx1_']),
            "transactionNumberInSecondBusStopWithMoreValidations": float(
                row['trx2_']),
            "transactionNumberInThirdBusStopWithMoreValidations": float(
                row['trx3_']),
            "transactionNumberInFourthBusStopWithMoreValidations": float(
                row['trx4_']),
            "transactionNumberInFifthBusStopWithMoreValidations": float(
                row['trx5_']),
            "transactionNumberInSixthBusStopWithMoreValidations": float(
                row['trx6_']),
            "transactionNumberInSeventhBusStopWithMoreValidations": float(
                row['trx7_']),
            "transactionNumberInEighthBusStopWithMoreValidations": float(
                row['trx8_']),
            "transactionNumberInNinethBusStopWithMoreValidations": float(
                row['trx9_']),
            "transactionNumberInTenthBusStopWithMoreValidations": float(
                row['trx10_'])
        }
