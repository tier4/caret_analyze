try:
    from pydantic import validate_arguments
    type_check_decorator = validate_arguments(config=dict(arbitrary_types_allowed=True))
except ModuleNotFoundError:
    def empty_decorator(func):
        def _wrapper(*args, **kargs):
            return func(*args, **kargs)
        return _wrapper
    type_check_decorator = empty_decorator
