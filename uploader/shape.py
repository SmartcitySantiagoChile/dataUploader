# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from itertools import groupby

from rqworkers.dataUploader.uploader.datafile import DataFile, get_timestamp

import csv
import traceback


class ShapeFile(DataFile):
    """ Class that represents a shape file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)

    def make_docs(self):
        with self.get_file_object(encoding="latin-1") as f:
            reader = csv.DictReader(f, delimiter='|')
            try:
                # Group data using 'route' as key
                for route, points in groupby(reader, lambda point: point['route']):
                    points = list(points)
                    start_date = self.name_to_date()
                    path = self.get_path()
                    timestamp = get_timestamp()
                    points = [{
                        'segmentStart': int(p['segmentStart']),
                        'longitude': float(p['longitude']),
                        'latitude': float(p['latitude'])} for p in points]
                    yield {
                        "_source": {
                            "path": path,
                            "timestamp": timestamp,
                            "route": route,
                            "startDate": start_date,
                            "points": points
                        }
                    }
            except ValueError:
                traceback.print_exc()

    def get_header(self):
        return 'route|segmentStart|latitude|longitude'
