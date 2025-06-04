

from datetime import datetime, timezone, timedelta
from traitlets import TraitType, Undefined
from traitlets.traitlets import _validate_bounds
from dateutil.parser import parse


class Datetime(TraitType):
    """A datetime trait."""

    default_value = datetime.fromtimestamp(86400)  # datetime.fromtimestamp(0) 不支持 windows
    info_text = 'a datetime'

    def __init__(self, default_value=Undefined, allow_none=False, **kwargs):
        self.min = kwargs.pop('min', None)
        self.max = kwargs.pop('max', None)
        super(Datetime, self).__init__(default_value=default_value, allow_none=allow_none, **kwargs)

    def validate(self, obj, value):
        if not isinstance(value, datetime):
            if isinstance(value, str):
                value = parse(value)
            else:
                self.error(obj, value)
        value.replace(tzinfo=timezone(timedelta(hours=8)))
        return _validate_bounds(self, obj, value)
