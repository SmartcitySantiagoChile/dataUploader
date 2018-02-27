#!/usr/bin/python3

from datafile import *


# Class that represents an expedition file.
class ExpeditionFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def makeDocs(self):
        with open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = self.getPath()
                timestamp = getTimeStamp()
                route = row['route']
                licensePlate = row['licensePlate']
                expeditionStartTime = row['expeditionStartTime']
                expeditionEndTime = row['expeditionEndTime']
                fulfillment = row['fulfillment']
                periodId = row['periodId']
                yield {
                    "_source": {
                        "path": path,
                        "timestamp": timestamp,
                        "route": route,
                        "licensePlate": licensePlate,
                        "expeditionStartTime": expeditionStartTime,
                        "expeditionEndTime": expeditionEndTime,
                        "fulfillment": fulfillment,
                        "periodId": periodId
                    }
                }

    def getHeader(self):
        return 'route|licensePlate|a|b|expeditionStartTime|expeditionEndTime|fulfillment|c|d|e|f|g|h|periodId'
