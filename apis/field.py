import datetime

import flask
import sqlalchemy
from flask import json
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs

from model.models import db
from utils import columns_dict, columns_dict_all, sql_query_generator
from .poll import poll_cache
from .flask_models import Info, info_field

api = Namespace('field',
                description='Operations related to fields (i.e., attributes from the Genomic Conceptual Model)')

field = api.model('Field', {
    'name': fields.String(attribute='column_name', required=True, description='Field name '),
    'group': fields.String(attribute='table_name', description='Field group '),
    'view': fields.String(attribute='view', description='Field view '),
    'description': fields.String(attribute='description', description='Field description '),
    'title': fields.String(attribute='title', description='Field title '),
    'is_numerical': fields.Boolean(attribute='is_numerical',
                                   description='True if field is numerical, False otherwise '),
    'is_date': fields.Boolean(attribute='is_date', description='True if field is a date, False otherwise '),
})

field_list = api.model('Fields', {
    'fields': fields.List(fields.Nested(field, required=True, description='Fields', skip_none=True)),
    'info': info_field,
})


@api.route('')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        """List all available fields with description and the group they belong to"""
        res = columns_dict.values()
        res = list(res)
        # TAG:AGE
        # age_item = [x for x in res if x.column_name == 'age'][0]
        # res.pop(res.index(age_item))
        # res.append(age_item)
        res_len = len(res)
        info = Info(res_len, res_len)
        res = {'fields': res, 'info': info}

        return res


value = api.model('Value', {
    'value': fields.Raw(required=True, description='Value '),
    'count': fields.Integer(required=False, description='Count '),
})

values = api.model('Values', {
    'values': fields.Nested(value, required=True, description='Values'),
    'info': info_field,
})

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean,
                    help='Enable inclusion of controlled vocabulary terms and synonyms (true/false)', default=False)

parser_body = api.parser()
parser_body.add_argument('body', type="json", help='json ', location='json')
parser_body.add_argument('rel_distance', type=int, default=3)

body_desc = 'It must be in the format {\"gcm\":{},\"type\":\"original\",\"kv\":{}}.\n ' \
            'Example values for the three parameters: \n ' \
            '- gcm may contain \"disease\":[\"prostate adenocarcinoma\",\"prostate cancer\"],\"assembly\":[\"grch38\"]\n ' \
            '- type may be original, synonym or expanded\n ' \
            '- kv may contain \"tumor_0\":{\"type_query\":\"key\",\"exact\":false,\"query\":{\"gcm\":{},\"pairs\":{\"biospecimen__bio__tumor_descriptor\":[\"metastatic\"]}}}'

rel_distance_hyper_desc = 'When type is \'expanded\', it indicates the depth of hypernyms in the ontological hierarchy to consider.'



@api.route("/numerical/<field_name>")
class Numerical(Resource):
    @api.doc('return_num_interval', params={'body': body_desc,
                                            'rel_distance': rel_distance_hyper_desc,
                                            'field_name': 'The requested metadata field.'})
    @api.expect(parser_body)
    def post(self, field_name):
        """For the posted query, returns minimum and maximum values of numerical field"""
        args = parser_body.parse_args()
        payload = api.payload
        filter_in = payload.get("gcm")
        pair_query = payload.get("kv")

        panel = payload.get("panel")

        if field_name in columns_dict_all:
            column = columns_dict_all[field_name]
            column_name = column.column_name

            query = gen_query_field(field_name, 'original', filter_in, pair_query, panel=panel)

            res = db.engine.execute(query).fetchall()
            flask.current_app.logger.debug(query)

            if column.column_type == datetime:
                res = [str(row['label']) for row in res if row['label'] is not None]
            else:
                res = [row['label'] for row in res if row['label'] is not None]

            if res:
                result = {
                    'max_val': max(res),
                    'min_val': min(res)
                }
            else:
                result = {
                    'max_val': "",
                    'min_val': ""
                }

            return result
        else:
            api.abort(404)


@api.route('/<field_name>')
@api.response(404, 'Field not found')
class FieldValue(Resource):

    @api.doc('post_value_list', params={'body': body_desc,
                                        'rel_distance': rel_distance_hyper_desc,
                                        'field_name': 'The requested GCM metadata field.'})
    @api.expect(parser_body)
    def post(self, field_name):
        """For a specified field, it lists all possible values"""

        args = parser_body.parse_args()
        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pair_query = payload.get("kv")
        rel_distance = args['rel_distance']

        panel = payload.get("panel")

        if field_name in columns_dict_all:
            poll_id = poll_cache.create_dict_element()

            def async_function():
                try:
                    column = columns_dict_all[field_name]
                    column_name = column.column_name
                    has_tid = column.has_tid
                    if type == 'original':
                        query = gen_query_field(field_name, type, filter_in, pair_query, panel=panel)

                        #if field_name == "product":
                        #    print("qui22", query)

                        res = db.engine.execute(query).fetchall()
                        flask.current_app.logger.debug(query)
                        item_count = sum(map(lambda row: row['item_count'], res))

                        if column.column_type == datetime:
                            res = [{'value': str(row['label']), 'count': row['item_count']} for row in res]
                        else:
                            res = [{'value': row['label'], 'count': row['item_count']} for row in res]

                        length = len(res)

                        # info = Info(length, length, item_count)

                        res = {'values': res,
                               # 'info': info
                               }
                    else:
                        query = gen_query_field(field_name, type, filter_in, pair_query, rel_distance)
                        flask.current_app.logger.debug(query)
                        res = db.engine.execute(query).fetchall()
                        item_count = sum(map(lambda row: row['item_count'], res))
                        res = [{'value': row['label'], 'count': row['item_count']} for row in res]

                        length = len(res)
                        info = Info(length, length, item_count)

                        res = {'values': res,
                               # 'info': info
                               }
                    poll_cache.set_result(poll_id, res)
                except Exception as e:
                    poll_cache.set_result(poll_id, None)
                    raise e

            from app import executor_inner
            executor_inner.submit(async_function)
            return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')
        else:
            flask.current_app.logger.debug(f"404: field {field_name} not found")
            api.abort(404)


def gen_query_field(field_name, type, filter_in, pair_query, rel_distance=3, panel=None):
    column = columns_dict_all[field_name]
    column_name = column.column_name
    has_tid = column.has_tid
    if type == 'original':
        filter_in_new = {x: filter_in[x] for x in filter_in if x != column_name}
        if panel:
            panel_new = {x: panel[x] for x in panel if x != column_name}
            print('panel_new', panel_new)
            if len(panel_new) == 0:
                panel = None
        else:
            panel_new = None
        sub_query1 = sql_query_generator(filter_in_new, pairs_query=pair_query, search_type=type,
                                         return_type='field_value', field_selected=field_name, panel=panel_new)
        group_by = " group by label "
        order_by = " order by item_count desc, label asc "
        select_part = f"SELECT label, count(*) as item_count "
        from_part = "FROM (" + sub_query1 + ") as view"

        query = select_part + from_part + group_by + order_by
        return sqlalchemy.text(query)
    else:
        filter_in_new = {x: filter_in[x] for x in filter_in if x != column_name}
        sub_query1 = sql_query_generator(filter_in_new, pairs_query=pair_query, search_type=type,
                                         return_type='field_value', field_selected=field_name)
        sub_query2 = ""
        if has_tid:
            sub_query2 = sql_query_generator(filter_in_new, search_type=type, pairs_query=pair_query,
                                             return_type='field_value_tid', field_selected=field_name,
                                             rel_distance=rel_distance)
        select_part = f"SELECT label, count(*) as item_count "
        if has_tid:
            from_part = "FROM (" + sub_query1 + " union " + sub_query2 + ") as view"
        else:
            from_part = "FROM (" + sub_query1 + ") as view"
        group_by = " group by label "
        order_by = " order by item_count desc, label asc "
        query = select_part + from_part + group_by + order_by
        return sqlalchemy.text(query)
