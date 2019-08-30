from pendulum import parse, instance, DateTime, Date, now


DATE_TYPES = {DateTime, Date}


def get(dt, date_only=False, **options):
    if isinstance(dt, str):
        parsed = parse(dt, **options)
    else:
        parsed = instance(dt, **options)
    if date_only:
        return parsed.date()
    return parsed


def utcnow() -> DateTime:
    return now('UTC')
