from collections import namedtuple
import gzip

from flask import json, make_response, Response
from flask_restplus import Namespace, Resource

import uuid

from collections.abc import MutableMapping

api = Namespace('poll', description='Operations to perform polling')


class Cache(MutableMapping):
    CacheValue = namedtuple('CacheValue', 'data ready')

    # from DictTTL.DictTTL import DictTTL
    # dict_ttl = DictTTL(5, data)

    def __init__(self, *args, **kwargs):
        self.poll_dict = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.poll_dict[key]

    def __contains__(self, key):
        return key in self.poll_dict

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        del self.poll_dict[key]

    def __iter__(self):
        return iter(self.poll_dict)

    def __len__(self):
        return len(self.poll_dict)

    def upsert_dict(self, poll_id, result_dict):
        result_json_zip = self.zip_result(result_dict)
        poll_value = self.CacheValue(result_json_zip, result_dict['ready'])
        self.poll_dict[poll_id] = poll_value
        return poll_id

    def create_dict_element(self):
        result_dict = {'ready': False}
        poll_id = self.generate_uuid()
        return self.upsert_dict(poll_id, result_dict)

    def set_result(self, poll_id, result):
        result_dict = {'ready': True, 'result': result}
        return self.upsert_dict(poll_id, result_dict)

    @staticmethod
    def generate_uuid():
        return uuid.uuid4().hex

    @staticmethod
    def zip_result(result_dict):
        result_json = json.dumps(result_dict, separators=(',', ':'))
        result_json_zip = gzip.compress(result_json.encode('utf8'), 9)
        return result_json_zip


poll_cache = Cache()


@api.route('/<poll_id>')
class Poll(Resource):
    def get(self, poll_id):
        if poll_id in poll_cache:
            result = poll_cache[poll_id]
            result_data = result.data
            if result.ready:
                del poll_cache[poll_id]
            response = make_response(result_data)
            response.headers['Content-length'] = len(result_data)
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['content-type'] = 'application/json'
            return response
        else:
            api.abort(404)


################################################################################################
import sys
from types import ModuleType, FunctionType
from gc import get_referents

# Custom objects know their class.
# Function objects seem to know way too much, including modules.
# Exclude modules as well.
BLACKLIST = type, ModuleType, FunctionType


def getsize(obj):
    """sum size of object & members."""
    if isinstance(obj, BLACKLIST):
        raise TypeError('getsize() does not take argument of type: ' + str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return size
################################################################################################
