#!/usr/bin/python3

from datafile import *


# Class that represents a speed file.
class SpeedFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def makeDocs(self):
        with open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            for row in reader:
                path = os.path.basename(self.datafile)
                timestamp = datetime.now()
                merged = str(row['route'] + '-' + row['section'] + '-' + row['periodId'])
                yield {"_source": dict(timestamp=timestamp, path=path, merged=merged, **row)}

    def getHeader(self):
        return 'route|section|date|periodId|dayType|totalDistance|totalTime|speed|nObs|nInvalidObs'