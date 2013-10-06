from __future__ import absolute_import

from datetime import datetime
import json

from psycopg2.extras import Json as BaseJson


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class Json(BaseJson):
    def dumps(self, obj):
        return json.dumps(obj, cls=DateTimeEncoder)


