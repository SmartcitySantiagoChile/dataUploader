#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from itertools import groupby

from reader.datafile import *


# Class that represents a shape file.
class ShapeFile(DataFile):
    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def makeDocs(self):
        # Get filename and extension
        filename, file_extension = os.path.basename(self.datafile).split(".")
        with open(self.datafile, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            # Group data using 'route' as key
            for route, points in groupby(reader, lambda p: p['route']):
                points = list(points)
                startDate = name_to_date(filename)
                path = self.getPath()
                timestamp = get_timestamp()
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
