#!/usr/bin/python3

from itertools import groupby

from datafile import *


# Class that represents a shape file.
class ShapeFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def makeDocs(self):
        # Get filename and extension
        filename, file_extension = os.path.basename(self.datafile).split(".")
        with open(self.datafile, "r") as f:
            reader = csv.DictReader(f, delimiter='|')
            # Group data using 'route' as key
            for route, points in groupby(reader, lambda p: p['route']):
                points = list(points)
                startDate = nameToDate(filename)
                path = os.path.basename(self.datafile)
                timestamp = datetime.now()
                points = [
                    {
                        'segmentStart': p['segmentStart'],
                        'longitude': p['longitude'],
                        'latitude': p['latitude']
                    } for p in points
                ]
                yield {
                    "_source": {
                        "path": path,
                        "timestamp": timestamp,
                        "route": route,
                        "startDate": startDate,
                        "points": points
                    }
                }

    def getHeader(self):
        return 'route|segmentStart|latitude|longitude'