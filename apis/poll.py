import gzip
import time
import uuid
from collections import namedtuple
from collections.abc import MutableMapping
from datetime import date
from typing import Iterable

from DictTTL.DictTTL import DictTTL
from flask import json, make_response
from flask_restplus import Namespace, Resource
from sqlalchemy.engine import RowProxy

api = Namespace('poll', description='Operations to perform polling')


class MyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Iterable):
            return list(obj)
        elif isinstance(obj, RowProxy):
            return list(obj)
        elif isinstance(obj, date):
            return str(obj)
        else:
            return super(MyJSONEncoder, self).default(obj)


class Cache(MutableMapping):
    CacheValue = namedtuple('CacheValue', 'data ready')

    time_to_live = 3600  # 1 hour
    purge_cycle_time = 300  # 5 minutes
    _last_purge = time.time()  # now

    def __init__(self, *args, **kwargs):
        self.poll_dict = DictTTL(self.time_to_live)
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        # print([(x, self.poll_dict.get_ttl(x)) for x in self.poll_dict.keys()])
        self.poll_dict.set_ttl(key, self.time_to_live)
        return self.poll_dict[key]

    def __contains__(self, key):
        return key in self.poll_dict

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        try:
            del self.poll_dict[key]
        except:
            pass

    def __iter__(self):
        return iter(self.poll_dict)

    def __len__(self):
        return len(self.poll_dict)

    def _purge(self):
        # print("_purge called")
        now = time.time()
        if self._last_purge + self.purge_cycle_time < now:
            self._last_purge = now
            try:
                # to trigger purge
                length = len(self)
                # print("_purge applied and length: ", length)
            except:
                pass
        # else:
        #     print("_purge skipped")

    def upsert_dict(self, poll_id, result_dict):
        result_json_zip = self.dump_and_zip_result(result_dict)
        poll_value = self.CacheValue(result_json_zip, result_dict['ready'])
        self.poll_dict[poll_id] = poll_value
        self._purge()
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
    def dump_and_zip_result(result_dict):
        result_json = json.dumps(result_dict, separators=(',', ':'), cls=MyJSONEncoder, sort_keys=False)
        result_json_zip = gzip.compress(result_json.encode('utf8'), 9)
        return result_json_zip


poll_cache = Cache()


@api.route('/<poll_id>')
class Poll(Resource):
    def get(self, poll_id):
        if "test" in poll_id:
            with open(f'{poll_id}.json', 'r') as infile:
                json_string = infile.read()
            result_data = gzip.compress(json_string.encode('utf8'), 9)
            print(len(json_string), len(result_data), len(json_string) / len(result_data))
            response = make_response(result_data)
            response.headers['Content-length'] = len(result_data)
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['content-type'] = 'application/json'
            return response

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
            api.abort(404, f"Requested ID ({poll_id}) is not available")

#
# ################################################################################################
# import sys
# from types import ModuleType, FunctionType
# from gc import get_referents
#
# # Custom objects know their class.
# # Function objects seem to know way too much, including modules.
# # Exclude modules as well.
# BLACKLIST = type, ModuleType, FunctionType
#
#
# def getsize(obj):
#     """sum size of object & members."""
#     if isinstance(obj, BLACKLIST):
#         raise TypeError('getsize() does not take argument of type: ' + str(type(obj)))
#     seen_ids = set()
#     size = 0
#     objects = [obj]
#     while objects:
#         need_referents = []
#         for obj in objects:
#             if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
#                 seen_ids.add(id(obj))
#                 size += sys.getsizeof(obj)
#                 need_referents.append(obj)
#         objects = get_referents(*need_referents)
#     return size
# ################################################################################################
