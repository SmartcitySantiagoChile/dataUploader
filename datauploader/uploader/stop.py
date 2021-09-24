import csv
from collections import defaultdict

from datauploader.errors import StopDocumentExist
from datauploader.uploader.datafile import DataFile


class StopFile(DataFile):
    """ Class that represents a stop file. """

    def __init__(self, datafile):
        DataFile.__init__(self, datafile)
        self.fieldnames = ['Servicio', 'ServicioUsuario', 'Operador', 'Correlativo', 'Codigo', 'CodigoUsuario',
                           'Nombre', 'Latitud', 'Longitud', "esZP"]
        self.uploaded_stops = []
        self.routes_by_stop = defaultdict(lambda: set())

        with self.get_file_object() as f:
            next(f)  # skip header
            delimiter = '|'
            reader = csv.DictReader(f, delimiter=delimiter, fieldnames=self.fieldnames)
            for row in reader:
                self.routes_by_stop[row['Codigo']].add(row['ServicioUsuario'])

        for authStopCode in self.routes_by_stop.keys():
            route_list = list(self.routes_by_stop[authStopCode])
            route_list.sort()
            self.routes_by_stop[authStopCode] = route_list

    def row_parser(self, row, path, timestamp):
        if row['CodigoUsuario'] in self.uploaded_stops or row['CodigoUsuario'] == '-':
            raise StopDocumentExist('Stop {0} exists'.format(row['CodigoUsuario']))

        self.uploaded_stops.append(row['CodigoUsuario'])

        return {
            'path': path,
            'timestamp': timestamp,
            'startDate': self.name_to_date(),
            'authCode': row['Servicio'],
            'userCode': row['CodigoUsuario'],
            'name': row['Nombre'],
            'routes': self.routes_by_stop[row['Codigo']],
            'latitude': float(row['Latitud']),
            'longitude': float(row['Longitud'])
        }
