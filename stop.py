#!/usr/bin/python3

from datafile import *


# Class that represents a stop file.
class StopFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def makeDocs(self):
        # Get filename and extension
        filename, file_extension = os.path.basename(self.datafile).split(".")
        with open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = os.path.basename(self.datafile)
                timestamp = datetime.now()
                date = nameToDate(filename)
                yield {"_source": dict(timestamp=timestamp, path=path, date=date, **row)}

    def getHeader(self):
        return 'authRouteCode|userRouteCode|operator|order|authStopCode|userStopCode|stopName|latitude|longitude'
