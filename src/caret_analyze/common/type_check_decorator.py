try:
    from pytypes import typechecked
    type_check_decorator = typechecked
except ModuleNotFoundError:
    def empty_decorator(f):
        def _wrapper(*args, **kargs):
            return f(*args, **kargs)
        return _wrapper
    type_check_decorator = empty_decorator
