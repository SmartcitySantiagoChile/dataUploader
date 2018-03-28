# -*- coding: utf-8 -*-
from __future__ import unicode_literals


class UnrecognizedFileExtensionError(Exception):
    pass


class IndexNotEmptyError(ValueError):
    pass


class StopDocumentExist(Exception):
    pass