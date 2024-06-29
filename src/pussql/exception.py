class Rollback(Exception):
    pass


class QueryFileNotFound(Exception):
    pass


class PussqlError(Exception):
    pass


class RecordNotFound(PussqlError):
    pass
