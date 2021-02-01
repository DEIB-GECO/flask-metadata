import datetime

import flask
import sqlalchemy
from flask import Response, json
from flask_restplus import Namespace, Resource, fields, inputs

from model.models import db
from .poll import poll_cache

from utils import sql_query_generator, epitope_table

is_gisaid = False

api = Namespace('epitope', description='epitope')

query = api.model('epitope', {
})


class ColumnEpi:

    def __init__(self, text, field, description, type, is_numerical=False, is_percentage=False):
        self.text = text
        self.field = field
        self.description = description
        self.type = type
        self.is_numerical = is_numerical
        self.is_percentage = is_percentage

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)


columns_epi_sel = [
    ColumnEpi('Protein Name', 'product', 'a', 'str', False, False),
    ColumnEpi('Assay (T/B Cell)', 'cell_type', 'b', 'str', False, False),
    ColumnEpi('HLA restriction', 'mhc_allele', 'c', 'str', False, False),
    ColumnEpi('Linear / Non Linear', 'is_linear', 'd', 'str', False, False),
    ColumnEpi('Response Freq', 'response_frequency', 'e', 'num', False, True),
    ColumnEpi('Position Range', 'position_range', 'f', 'num', True, False),
]

columns_epi_amino = [
    ColumnEpi('Variant Type', 'variant_aa_type', 'aa', 'str', False, False),
    ColumnEpi('Original Aminoacid', 'sequence_aa_original', 'bb', 'str', False, False),
    ColumnEpi('Alternative Aminoacid', 'sequence_aa_alternative', 'cc', 'str', False, False),
    ColumnEpi('Variant Position Range', 'variant_position_range', 'dd', 'num', True, False),
]

columns_dict_epi_sel = {x.field: x for x in columns_epi_sel}

columns_dict_epi_amino = {x.field: x for x in columns_epi_amino}

columns_dict_epi_all = {x.field: x for x in columns_epi_sel + columns_epi_amino}


field = api.model('Field', {
    'text': fields.String(attribute='text', required=True, description='Name of the Field '),
    'field': fields.String(attribute='field', description='Field in table '),
    'description': fields.String(attribute='description', description='Field description '),
    'is_numerical': fields.Boolean(attribute='is_numerical',
                                   description='True if field is numerical, False otherwise '),
    'is_percentage': fields.Boolean(attribute='is_percentage', description='True if field is a percentage'
                                                                           ', False otherwise '),
})

field_list = api.model('Fields', {
    'fields': fields.List(fields.Nested(field, required=True, description='Fields', skip_none=True))
})


########################################################################################################


@api.route('')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        res = columns_dict_epi_sel.values()
        res = list(res)
        res = {'fields': res}
        return res


@api.route('/fieldAminoEpi')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        res = columns_dict_epi_amino.values()
        res = list(res)
        res = {'fields': res}
        return res


@api.route('/epiExtremes/<field_name>')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self, field_name):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                if field_name == 'position_range':
                    query_ex = f"""SELECT min(distinct(epic.epi_annotation_start)) as min, 
                                max(distinct(epic.epi_annotation_stop)) as max
                                FROM {epitope_table} as epic"""

                elif field_name == 'variant_position_range':
                    query_ex = f"""SELECT min(distinct(epic.start_aa_original)) as min, 
                                max(distinct(epic.start_aa_original + (epic.variant_aa_length - 1))) as max
                                FROM {epitope_table} as epic"""

                query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                panel, payload_epi_query, field_name)

                query_ex_2 = sqlalchemy.text(query_ex)
                res = db.engine.execute(query_ex_2).fetchall()
                flask.current_app.logger.debug(query_ex_2)

                res = [{'start': row['min'], 'stop': row['max']} for row in res]
                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/epiFreqExtremes')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)
        poll_id = poll_cache.create_dict_element()
        field_name = "response_frequency"

        def async_function():
            try:
                query_ex = f"""SELECT min(distinct(epic.response_frequency_pos)) as min,
                                max(distinct(epic.response_frequency_pos)) as max,
                                count(*) as count
                                FROM {epitope_table} as epic"""

                query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                panel, payload_epi_query, field_name)

                query_ex_2 = sqlalchemy.text(query_ex)
                res = db.engine.execute(query_ex_2).fetchall()
                flask.current_app.logger.debug(query_ex_2)

                res = [{'startFreq': row['min'], 'stopFreq': row['max'], 'count': row['count']} for row in res]
                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/epiSel/<field_name>')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self, field_name):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_ex = """SELECT label, count(*) as item_count FROM (
                                SELECT distinct 
                                ("""
                query_ex += field_name
                query_ex += f""") as label, epitope_id as item
                            FROM {epitope_table}"""

                query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                panel, payload_epi_query, field_name)

                query_ex += """) as view
                group by label
                order by item_count desc, label asc"""

                query_ex_2 = sqlalchemy.text(query_ex)
                res = db.engine.execute(query_ex_2).fetchall()
                flask.current_app.logger.debug(query_ex_2)

                res = [{'value': row['label'], 'count': row['item_count']} for row in res]
                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/epiTableRes')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)
        payload_table_headers = payload.get("table_headers")

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_table = ""
                query_table += gen_select_epi_query_table(payload_table_headers)

                query_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query_table += """ GROUP BY epitope_id
                                ORDER BY epitope_id"""

                query = sqlalchemy.text(query_table)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{column: value for column, value in row.items()} for row in res]
                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/count')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_count_table = f"SELECT count(distinct epitope_id) as count_epi FROM {epitope_table} "

                query_count_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query = sqlalchemy.text(query_count_table)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{'count': row['count_epi']} for row in res]

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/countSeq')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)

        poll_id = poll_cache.create_dict_element()
        def async_function():
            try:
                query_count_table = f"SELECT count(distinct sequence_id) as count_seq FROM {epitope_table} "

                query_count_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query = sqlalchemy.text(query_count_table)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{'count': row['count_seq']} for row in res]

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


############


############ FUNZIONI


def get_payload(payload):
    payload_epi_query = payload.get("epi_query")
    payload_cmp_query = payload.get("compound_query")
    filter_in = payload_cmp_query.get("gcm")
    type = payload_cmp_query.get("type")
    pair_query = payload_cmp_query.get("kv")
    panel = payload_cmp_query.get("panel")
    return payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel


def gen_where_epi_query_field(payload_epi_query, field_name):

    if field_name == "toTable":
        where_part = " WHERE "
    else:
        where_part = ""

    i = 0;

    for (column, values) in payload_epi_query.items():
        if column == "epitope_id":
            where_part += add_and(i, field_name)
            where_part += f" epitope_id = {values} "
        elif column == "startExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epi_annotation_stop >= {value} "
        elif column == "stopExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epi_annotation_start <= {value} "
        elif column == "startExtVariant":
            if field_name != "variant_position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" start_aa_original >= {value} "
        elif column == "stopExtVariant":
            if field_name != "variant_position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" start_aa_original <= {value} "
        elif column == "startFreqExt" or column == "stopFreqExt":
            if column == "startFreqExt":
                if field_name != "response_frequency":
                    for value in values:
                        if value is not None:
                            where_part += add_and( i, field_name)
                            where_part += f" response_frequency_pos >= {value} "
                        else:
                            where_part += add_and( i, field_name)
                            where_part += f" response_frequency_pos IS NULL "
            elif column == "stopFreqExt":
                if field_name != "response_frequency":
                    for value in values:
                        if value is not None:
                            where_part += add_and( i, field_name)
                            where_part += f" response_frequency_pos <= {value} "
                        else:
                            where_part += add_and( i, field_name)
                            where_part += f" response_frequency_pos IS NULL "
        else:
            col = columns_dict_epi_all[column]
            #col = columns_dict_epi_sel[column]
            column_type = col.type
            if column != field_name:
                where_part += add_and( i, field_name)
                where_part += f" ("
                count = len(values)
                for value in values:
                    if value is not None:
                        where_part += f" {column} ="
                        if column_type == 'str':
                            where_part += f" '{value}' "
                        elif column_type == 'num':
                            where_part += f" {value} "
                        else:
                            where_part += ""
                    else:
                        where_part += f" {column} IS NULL "
                    count = count - 1
                    if count > 0:
                        where_part += f" or "
                where_part += f" ) "
        i = i + 1

    return where_part


def add_and(i, field_name):
    if i == 0 and field_name == "toTable":
        where_part = ""
    else:
        where_part = " and "
    return where_part


def add_where_epi_query(filter_in, pairs_query, search_type, return_type,
                        field_selected, panel, payload_epi_query, field_name):

    where_part_final = f" WHERE sequence_id IN ("
    query_seq_sel = sql_query_generator(filter_in, pairs_query=pairs_query, search_type=search_type,
                                        return_type=return_type, field_selected=field_selected, panel=panel)
    where_part_final += query_seq_sel

    where_part_final += f") "
    if payload_epi_query is not None:
        epi_query_len = len(payload_epi_query)
        if epi_query_len != 0:
            query_where_epi = gen_where_epi_query_field(payload_epi_query, field_name)
            where_part_final += query_where_epi

    return where_part_final


def gen_select_epi_query_table(payload_table_headers):
    table_select_part = f"SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == 'epitope_id':
            table_select_part += f"epitope_id "
        elif header == 'num_seq':
            table_select_part += f"count(distinct(sequence_id)) as {header}"
        elif header == 'num_var':
            table_select_part += f"sum(variant_aa_length ) as {header}"
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            table_select_part += f"array_agg(distinct row(epi_fragment_id, " \
                                 f"{header}) order by (epi_fragment_id, {header})) as {header}"
        else:
            table_select_part += f"array_agg(distinct {header}) as {header}"

        count = count - 1
        if count > 0:
            table_select_part += ', '

    table_select_part += f" FROM {epitope_table} "

    return table_select_part