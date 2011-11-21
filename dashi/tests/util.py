import sys

def who_is_calling():
    """Returns the name of the caller's calling function.

    Just a hacky way to pin things to test method names.
    There must be a better way.
    """
    return sys._getframe(2).f_code.co_name
