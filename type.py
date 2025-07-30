from typing import Type, cast, TypeVar

T = TypeVar('T')

def checked_as(expr: object, typ: Type[T]) -> T:
    if not isinstance(expr, typ):
        raise TypeError
    return cast(T, expr)