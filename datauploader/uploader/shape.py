import csv
import traceback
from itertools import groupby

from datauploader.uploader.datafile import DataFile, get_timestamp


class ShapeFile(DataFile):
    """ Class that represents a shape file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['authRouteCode', 'segmentStart', 'latitude', 'longitude', 'operator', 'userRouteCode']

    def row_parser(self, row, path, timestamp):
        pass

    def make_docs(self):
        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = str('|')
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            # Group data using 'route' as key
            for identifier, points in groupby(reader, lambda point: (point['authRouteCode'], point['userRouteCode'],
                                                                     point['operator'])):
                try:
                    points = list(points)
                    start_date = self.name_to_date()
                    path = self.basename
                    timestamp = get_timestamp()
                    points = [{
                        'segmentStart': int(p['segmentStart']),
                        'longitude': float(p['longitude']),
                        'latitude': float(p['latitude'])} for p in points]
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
