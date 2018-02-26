#!/usr/bin/python3

from datafile import *


# Class that represents a profile file.
class ProfileFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)


    def makeDocs(self):
        with open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = os.path.basename(self.datafile)
                timestamp = datetime.now()
                operator = row['operator']
                route = row['route']
                userRoute = row['userRoute']
                shapeRoute = row['shapeRoute']
                licensePlate = row['licensePlate']
                authStopCode = row['authStopCode']
                userStopName = row['userStopName']
                expeditionStartTime = row['expeditionStartTime']
                expeditionEndTime = row['expeditionEndTime']
                fulfillment = row['fulfillment']
                expeditionStopOrder = row['expeditionStopOrder']
                expeditionDayId = row['expeditionDayId']
                stopDistanceFromPathStart = row['stopDistanceFromPathStart']
                expandedBoarding = row['expandedBoarding']
                expandedAlighting = row['expandedAlighting']
                loadProfile = row['loadProfile']
                busCapacity = row['busCapacity']
                expeditionStopTime = row['expeditionStopTime']
                userStopCode = row['userStopCode']
                timePeriodInStartTime = row['timePeriodInStartTime']
                timePeriodInStopTime = row['timePeriodInStopTime']
                if expeditionStopTime == '-':
                    expeditionStopTime = 0
                    timePeriodInStopTime = ""
                dayType = row['dayType']
                busStation = row['busStation']
                transactions = row['transactions']
                halfHourInStartTime = row['halfHourInStartTime']
                halfHourInStopTime = row['halfHourInStopTime']
                yield {
                    "_source": {
                        "path": path,
                        "timestamp": timestamp,
                        "operator": operator,
                        "route": route,
                        "userRoute": userRoute,
                        "shapeRoute": shapeRoute,
                        "licensePlate": licensePlate,
                        "authStopCode": authStopCode,
                        "userStopName": userStopName,
                        "expeditionStartTime":expeditionStartTime,
                        "expeditionEndTime": expeditionEndTime,
                        "fulfillment": fulfillment,
                        "expeditionStopOrder": expeditionStopOrder,
                        "expeditionDayId": expeditionDayId,
                        "stopDistanceFromPathStart": stopDistanceFromPathStart,
                        "expandedBoarding": expandedBoarding,
                        "expandedAlighting": expandedAlighting,
                        "loadProfile": loadProfile,
                        "busCapacity": busCapacity,
                        "expeditionStopTime": expeditionStopTime,
                        "userStopCode": userStopCode,
                        "timePeriodInStartTime": timePeriodInStartTime,
                        "timePeriodInStopTime": timePeriodInStopTime,
                        "dayType": dayType,
                        "busStation": busStation,
                        "transactions": transactions,
                        "halfHourInStartTime": halfHourInStartTime,
                        "halfHourInStopTime": halfHourInStopTime
                        }
                    }

    def getHeader(self):
        return 'operator|route|userRoute|shapeRoute|licensePlate|authStopCode|userStopName|expeditionStartTime|expeditionEndTime|fulfillment|expeditionStopOrder|expeditionDayId|stopDistanceFromPathStart|#Subidas|#SubidasLejanas|Subidastotal|expandedBoarding|#Bajadas|#BajadasLejanas|Bajadastotal|expandedAlighting|loadProfile|busCapacity|TiempoGPSInterpolado|TiempoPrimeraTrx|TiempoGPSMasCercano|expeditionStopTime|nSubidasTmp|userStopCode|timePeriodInStartTime|timePeriodInStopTime|dayType|busStation|transactions|halfHourInStartTime|halfHourInStopTime'