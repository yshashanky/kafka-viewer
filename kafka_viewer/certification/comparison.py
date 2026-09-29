from decimal import Decimal, InvalidOperation

from .models import MISSING


def type_name(value):
    return "missing" if value is MISSING else "null" if value is None else type(value).__name__


def numeric(value):
    if type(value) not in (int, float, str, Decimal):
        return None
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except InvalidOperation:
        return None


def value_equal(left, right):
    if (left is MISSING or left is None or left == "") and (right is MISSING or right is None or right == ""):
        return True
    if type(left) is type(right) and left == right:
        return True
    a, b = numeric(left), numeric(right)
    return a is not None and b is not None and a == b


def compare(source, destination):
    return [
        {"field": name, "source": source.data.get(name, MISSING),
         "destination": destination.data.get(name, MISSING),
         "value_equal": value_equal(source.data.get(name, MISSING), destination.data.get(name, MISSING)),
         "type_match": type_name(source.data.get(name, MISSING)) == type_name(destination.data.get(name, MISSING)),
         "source_type": type_name(source.data.get(name, MISSING)),
         "destination_type": type_name(destination.data.get(name, MISSING))}
        for name in sorted(source.data.keys() | destination.data.keys())
    ]
