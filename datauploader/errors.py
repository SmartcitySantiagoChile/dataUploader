class UnrecognizedFileExtensionError(Exception):
    pass


class IndexNotEmptyError(ValueError):
    pass


class StopDocumentExistError(ValueError):
    pass


class FilterDocumentError(ValueError):
    pass
