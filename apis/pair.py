import flask
import sqlalchemy
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs

from model.models import db
from utils import sql_query_generator, log_query

api = Namespace('pair', description='Operations to perform queries on key-value metadata pairs')

query = api.model('Pair', {
    'key': fields.String(attribute='column_name', required=True, description='Field name '),
})

parser = api.parser()
parser.add_argument('body', type="json", help='json ', location='json')
parser.add_argument('q', type=str)
parser.add_argument('exact', type=inputs.boolean, default=False)
parser.add_argument('rel_distance', type=int, default=3)

value_parser = api.parser()
value_parser.add_argument('body', type="json", help='json ', location='json')
value_parser.add_argument('is_gcm', type=inputs.boolean, default=True)
value_parser.add_argument('rel_distance', type=int, default=3)

################################API DOCUMENTATION STRINGS###################################
body_desc = 'It represents the context of the key-value query, based on the previous selection on gcm part.' \
            'It must be in the format {\"gcm\":{},\"type\":\"original\",\"kv\":{}}.\n ' \
            'Example values for the three parameters: \n ' \
            '- gcm may contain \"disease\":[\"prostate adenocarcinoma\",\"prostate cancer\"],\"assembly\":[\"grch38\"]\n ' \
            '- type may be original, synonym or expanded\n ' \
            '- kv may contain \"tumor_0\":{\"type_query\":\"key\",\"exact\":false,\"query\":{\"gcm\":{},\"pairs\":{\"biospecimen__bio__tumor_descriptor\":[\"metastatic\"]}}}'

qk_desc = 'The user input string to be searched in the keys.'
qv_desc = 'The user input string to be searched in the values.'

exactk_desc = 'Exact is false to retrieve keys which contain the input string.\n' \
              'Exact is true to retrieve keys which are equal to the input string.\n' \
              'The modulo \% can be used to express \"a string of any length and any character\".'

exactv_desc = 'Exact is false to retrieve values which contain the input string.\n' \
              'Exact is true to retrieve values which are equal to the input string.\n' \
              'The modulo \% can be used to express \"a string of any length and any character\".'

key_desc = 'Specific original key for which all available values are to be retrieved.'

is_gcm_desc = 'Is_gcm is false when the searched key is not in the Genomic Conceptual Model.\n' \
              'Is_gcm is true when the searched key is in the Genomic Conceptual Model.'

rel_distance_desc = 'When type is \'expanded\', it indicates the depth of hyponyms in the ontological hierarchy to consider.'


#############################SERVICES IMPLEMENTATION#############################################

@api.route('/keys')
@api.response(404, 'Results not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys', params={'body': body_desc,
                                 'q': qk_desc,
                                 'exact': exactk_desc})
    @api.expect(parser)
    def post(self):
        '''Retrieves all keys based on a user input string'''
        args = parser.parse_args()
        key = args['q']
        exact = args['exact']
        rel_distance = args['rel_distance']

        key = key.replace("_", "\_")

        if not exact:
            q = f"%{key}%"
        else:
            q = key

        payload = api.payload

        log_query('pair/keys',q,payload)

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")
        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None, rel_distance=rel_distance)
        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        query_gcm = f"select up.key as key, " \
                        f" count(distinct up.value) as count, " \
                        f" array(select unnest(array_agg(distinct up.value)) limit 10) as ex_values " + from_sub + \
                    f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                    f" and lower(up.key) like lower('{q}') and up.is_gcm = true " \
                        f" group by up.key"

        query_pair = f"select up.key as key, count(distinct up.value) as count " + from_sub + \
                     f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                     f" and lower(up.key) like ('{q}') and up.is_gcm = false " \
                         f" group by up.key"

        flask.current_app.logger.debug(query_gcm)
        flask.current_app.logger.debug(query_pair)
        res_gcm = db.engine.execute(sqlalchemy.text(query_gcm)).fetchall()
        res_pair = db.engine.execute(sqlalchemy.text(query_pair)).fetchall()
        results_gcm = []
        results_pairs = []

        for r in res_gcm:
            results_gcm.append({'key': r.key, 'count_values': r.count, 'values': r.ex_values})

        for r in res_pair:
            results_pairs.append({'key': r.key, 'count_values': r.count})

        results = {'gcm': results_gcm, 'pairs': results_pairs}

        return results


@api.route('/<key>/values')
@api.response(404, 'Results not found')  # TODO correct
class Key(Resource):
    @api.doc('get_values_for_key', params={'body': body_desc,
                                           'key': key_desc,
                                           'is_gcm': is_gcm_desc, })
    @api.expect(value_parser)
    def post(self, key):
        '''For a specific key, it lists all available values'''
        args = value_parser.parse_args()
        is_gcm = args['is_gcm']

        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")
        rel_distance = args['rel_distance']

        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None, rel_distance=rel_distance)

        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        query = f"select up.value as value, count(distinct it.item_id) as count " + from_sub + " " \
                " join unified_pair up on it.item_id = up.item_id " + where_sub + \
                f" and up.key = lower('{key}') and up.is_gcm = {is_gcm} " \
                " group by up.value"


        flask.current_app.logger.debug(query)
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()

        result = []

        for r in res:
            result.append({'value': r.value, 'count': r.count})

        return result


@api.route('/values')
@api.response(404, 'Results not found')  # TODO correct
class Key(Resource):
    @api.doc('get_values', params={'body': body_desc,
                                   'q': qv_desc,
                                   'exact': exactv_desc})
    @api.expect(parser)
    def post(self):
        '''Retrieves all values based on a user input string'''
        args = parser.parse_args()
        value = args['q']

        exact = args['exact']
        rel_distance = args['rel_distance']

        value = value.replace("_", "\_")
        if not exact:
            q = f"%{value}%"
        else:
            q = value

        payload = api.payload

        log_query('pair/values', q, payload)

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None, rel_distance=rel_distance)

        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        query = f"select up.key, up.value, up.is_gcm, count(distinct up.item_id) as count " + from_sub + \
                f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                f" and lower(up.value) like lower('{q}') " \
                    f" group by up.key, up.value, up.is_gcm"

        res = db.engine.execute(sqlalchemy.text(query)).fetchall()
        flask.current_app.logger.debug(query)
        results_gcm = []
        results_pairs = []
        i = 0
        j = 0
        for r in res:
            if r['is_gcm']:
                results_gcm.append({'key': r.key, 'value': r.value, 'count': r.count, 'id': i})
                i += 1
            else:
                results_pairs.append({'key': r.key, 'value': r.value, 'count': r.count, 'id': j})
                j += 1
        results = {'gcm': results_gcm, 'pairs': results_pairs}

        return results
