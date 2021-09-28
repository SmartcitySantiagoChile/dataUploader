import csv
import traceback
from itertools import groupby

from datauploader.uploader.datafile import DataFile, get_timestamp


class ShapeFile(DataFile):
    """ Class that represents a shape file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['Route', 'IsSectionInit', 'Latitude', 'Longitude', 'Operator', 'RouteUser']

    def row_parser(self, row, path, timestamp):
        pass

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            # Group data using 'route' as key
            for identifier, points in groupby(reader, lambda point: (point['Route'], point['RouteUser'],
                                                                     point['Operator'])):
                try:
                    points = list(points)
                    start_date = self.name_to_date()
                    path = self.basename
                    timestamp = get_timestamp()
                    points = [{
                        'segmentStart': int(p['IsSectionInit']),
                        'longitude': float(p['Longitude']),
                        'latitude': float(p['Latitude'])} for p in points]
                    yield {
                        "_source": {
                            "path": path,
                            "timestamp": timestamp,
                            "authRouteCode": identifier[0],
                            "userRouteCode": identifier[1],
                            "operator": identifier[2],
                            "startDate": start_date,
                            "points": points
                        }
                    }
                except ValueError:
                    traceback.print_exc()

