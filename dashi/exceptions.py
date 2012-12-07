class DashiError(Exception):
    def __init__(self, message=None, exc_type=None, value=None, traceback=None, **kwargs):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback

        if message is None:
            if exc_type:
                if value:
                    message = "%s: %s" % (exc_type, value)
                else:
                    message = exc_type
            elif value:
                message = value
            else:
                message = ""
            if traceback:
                message += "\n" + str(traceback)
        super(DashiError, self).__init__(message)


class BadRequestError(DashiError):
    pass


class NotFoundError(DashiError):
    pass


class UnknownOperationError(DashiError):
    pass


class WriteConflictError(DashiError):
    pass
