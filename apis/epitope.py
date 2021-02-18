import datetime

import flask
import sqlalchemy
from flask import Response, json
from flask_restplus import Namespace, Resource, fields, inputs

from model.models import db
from .poll import poll_cache

from utils import sql_query_generator, epitope_table, taxon_name_dict

is_gisaid = False
epitope_id = 'iedb_epitope_id'

api = Namespace('epitope', description='epitope')

query = api.model('epitope', {
})

table_parser = api.parser()
table_parser.add_argument('page', type=int)
table_parser.add_argument('num_elems', type=int)
table_parser.add_argument('order_col', type=str, default='accession_id')
table_parser.add_argument('order_dir', type=str, default='asc')


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

columns_user_new_epi_sel = [
    ColumnEpi('Epitope Name', 'epitope_name', 'aaa', 'str', False, False),
    ColumnEpi('Protein Name', 'product', 'bbb', 'str', False, False),
    ColumnEpi('Position Range', 'position_range', 'ccc', 'num', True, False),
]

columns_user_new_epi_amino = [
    ColumnEpi('Protein Name', 'product', 'aaaa', 'str', False, False),
    ColumnEpi('Variant Type', 'variant_aa_type', 'bbbb', 'str', False, False),
    ColumnEpi('Original Aminoacid', 'sequence_aa_original', 'cccc', 'str', False, False),
    ColumnEpi('Alternative Aminoacid', 'sequence_aa_alternative', 'dddd', 'str', False, False),
    ColumnEpi('Position Range', 'position_range', 'eeee', 'num', True, False),
]

columns_dict_epi_sel = {x.field: x for x in columns_epi_sel}

columns_dict_user_new_epi_sel = {x.field: x for x in columns_user_new_epi_sel}

columns_dict_epi_amino = {x.field: x for x in columns_epi_amino}

columns_dict_user_new_epi_amino = {x.field: x for x in columns_user_new_epi_amino}

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


@api.route('/newEpitopeFields')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        res = columns_dict_user_new_epi_sel.values()
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


@api.route('/fieldAminoNewEpiUser')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        res = columns_dict_user_new_epi_amino.values()
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


@api.route('/extremesPositionNewEpitope')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        product =payload.get('product')
        panel = payload.get('panel')

        poll_id = poll_cache.create_dict_element()
        def async_function():
            try:
                query_ex = f"""select min(start_aa_original) as min, max(start_aa_original) as max
                        from annotation as ann JOIN aminoacid_variant as amin ON ann.annotation_id = amin.annotation_id"""

                if product is not None:
                    query_ex += f""" WHERE LOWER(ann.product) = '{product}' """

                if panel is not None:
                    query_ex += add_where_panel_amino(panel, product)

                print("QUI40", query_ex)

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
                query_ex += f""") as label, {epitope_id} as item
                            FROM {epitope_table} as epic"""

                query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                panel, payload_epi_query, field_name)

                query_ex += """) as view
                group by label
                order by item_count desc, label asc"""

                query_ex_2 = sqlalchemy.text(query_ex)
                res = db.engine.execute(query_ex_2).fetchall()
                flask.current_app.logger.debug(query_ex_2)

                if field_name == 'mhc_allele':
                    list_separated_mhc_allele = []
                    for row in res:
                        all_mhc_allele = row['label']
                        count_mhc_allele = row['item_count']
                        if all_mhc_allele is not None:
                            list_mhc_allele = list(all_mhc_allele.split(','))
                            for item in list_mhc_allele:
                                row_dict = {'label': item, 'count': count_mhc_allele}
                                if not any(allele['label'] == item for allele in list_separated_mhc_allele):
                                    list_separated_mhc_allele.append(row_dict)
                                else:
                                    for allele in list_separated_mhc_allele:
                                        if allele['label'] == item:
                                            allele['count'] = allele['count'] + count_mhc_allele
                        else:
                            list_mhc_allele = None
                            row_dict = {'label': list_mhc_allele, 'count': count_mhc_allele}
                            list_separated_mhc_allele.append(row_dict)

                    list_separated_mhc_allele.sort(key=lambda s: s['count'], reverse=True)

                    res = [{'value': allele['label'], 'count': allele['count']} for allele in list_separated_mhc_allele]
                else:
                    res = [{'value': row['label'], 'count': row['item_count']} for row in res]
                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/epiTableResLimit')
@api.response(404, 'Field not found')
@api.expect(table_parser)
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        (payload_epi_query, payload_cmp_query, filter_in, type, pair_query, panel) = get_payload(payload)
        payload_table_headers = payload.get("table_headers")

        args = table_parser.parse_args()
        orderCol = args['order_col']
        orderDir = args['order_dir']
        if orderCol == "null":
            orderCol = f"{epitope_id}"

        numPage = args['page']
        numElems = args['num_elems']

        if numPage and numElems:
            offset = (numPage - 1) * numElems
            limit = numElems
        else:
            offset = None
            limit = None

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_table = ""
                query_table += gen_select_epi_query_table(payload_table_headers)

                query_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query_table += f""" GROUP BY {epitope_id}"""
                query_table += f" ORDER BY {orderCol} {orderDir} "

                if limit:
                    query_table += f" LIMIT {limit} "
                if offset:
                    query_table += f" OFFSET {offset} "

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

                query_table += f""" GROUP BY {epitope_id}
                                ORDER BY {epitope_id}"""

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


@api.route('/epiTableRes1')
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
                query_table += gen_select_epi_query_table1(payload_table_headers)

                query_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query_table += f""" GROUP BY {epitope_id}
                                ORDER BY {epitope_id}) as a"""

                #query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = a.epitope_id """

                query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = (SELECT min(epitope_id)
												   FROM epitope as c
													WHERE c.iedb_epitope_id = a.iedb_epitope_id)"""

                query_table += group_by_epi_query_table1(payload_table_headers)

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


@api.route('/epiTableRes2')
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
                query_table += gen_select_epi_query_table2(payload_table_headers)

                query_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query_table += f""" GROUP BY {epitope_id}
                                ORDER BY {epitope_id}"""

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
                query_count_table = f"SELECT count(distinct {epitope_id}) as count_epi FROM {epitope_table} as epic"

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
                query_count_table = f"SELECT count(distinct sequence_id) as count_seq FROM {epitope_table} as epic"

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


@api.route('/countVariantsEpitopeUser')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pair_query = payload.get("kv")
        panel = payload.get("panel")

        query_count_variant = sql_query_generator(filter_in, pairs_query=pair_query, search_type=type,
                                return_type="count_variants", field_selected="", panel=panel)

        query = sqlalchemy.text(query_count_variant)
        res = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res = [{'count': row['num_var']} for row in res]

        #res =[{'count': 0}]

        return res


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


def add_where_panel_amino(panel, product):

    where_part = ''

    length = len(panel)
    add_where_initial = False
    add_and_initial = False
    i = 0

    for key_panel in panel:
        if product is None and add_where_initial is False:
            where_part += ' WHERE '
            add_where_initial = True
        if product is not None and add_and_initial is False:
            where_part += ' AND '
            add_and_initial = True
        where_part += f""" LOWER({key_panel}) = '{panel.get(key_panel)}' """
        i = i + 1
        if i < length:
            where_part += """ AND """

    return where_part



def gen_where_epi_query_field(payload_epi_query, field_name):

    if field_name == "toTable":
        where_part = " WHERE "
    else:
        where_part = ""

    i = 0;

    for (column, values) in payload_epi_query.items():
        if column == f"{epitope_id}":
            where_part += add_and(i, field_name)
            where_part += f" {epitope_id} = {values} "
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
                        if column == "mhc_allele":
                            where_part += f"( mhc_allele LIKE '%{value},%' or mhc_allele LIKE '%{value}')"
                        else:
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
    where_part_final = ""

    where_part_final += f""" LEFT JOIN ( """
    query_seq_sel = sql_query_generator(filter_in, pairs_query=pairs_query, search_type=search_type,
                                        return_type=return_type, field_selected=field_selected, panel=panel)
    where_part_final += query_seq_sel
    where_part_final += f""" ) as seqc ON epic.sequence_id = seqc.sequence_id """

    where_part_final += f" WHERE "


    the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
    taxon_id = the_virus["taxon_id"]
    the_host = host_taxon_name_dict[filter_in['host_taxon_name'][0].lower()]
    host_taxon_id = the_host["host_taxon_id"]

    where_part_final += f""" taxon_id = {taxon_id} 
                and host_taxon_id = {host_taxon_id} """

    #where_part_final += f" and sequence_id IN ( "

    #query_seq_sel = sql_query_generator(filter_in, pairs_query=pairs_query, search_type=search_type,
    #                                    return_type=return_type, field_selected=field_selected, panel=panel)
    #where_part_final += query_seq_sel

    #where_part_final += f") "

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
        if header == f'{epitope_id}':
            table_select_part += f"{epitope_id} "
        elif header == 'num_seq':
            table_select_part += f"count(distinct(seqc.sequence_id)) as {header}"
        elif header == 'num_var':
            table_select_part += f"""sum(CASE
                                         WHEN epic.sequence_id = seqc.sequence_id THEN variant_aa_length
                                         ELSE 0
                                    END) as {header}"""
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            table_select_part += f"array_agg(distinct row(epi_fragment_id, " \
                                 f"{header}) order by (epi_fragment_id, {header})) as {header}"
        else:
            table_select_part += f"array_agg(distinct {header}) as {header}"

        count = count - 1
        if count > 0:
            table_select_part += ', '

    table_select_part += f" FROM {epitope_table} as epic"

    return table_select_part


def group_by_epi_query_table1(payload_table_headers):
    group_by_part = "GROUP BY "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == f'{epitope_id}':
            group_by_part += f"a.{header} "
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            group_by_part += ""
        else:
            group_by_part += f"{header}"

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop':
                group_by_part += ''
            else:
                group_by_part += ', '

    return group_by_part


def gen_select_epi_query_table1(payload_table_headers):
    table_select_part = f"SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == 'epi_fragment_sequence':
            table_select_part += f"""array_agg(distinct row(epi_frag_annotation_start,
                                        epi_frag_annotation_stop, {header}) 
                                        order by (epi_frag_annotation_start,
                                        epi_frag_annotation_stop, {header})) as epi_fragment_all_information """
        elif header == 'epi_frag_annotation_start' or header == 'epi_frag_annotation_stop':
            table_select_part += ""
        elif header == f'{epitope_id}':
            table_select_part += f" a.{header} "
        else:
            table_select_part += f" {header} "

        count = count - 1
        if count > 0:
            if header == 'epi_frag_annotation_start' or header == 'epi_frag_annotation_stop':
                table_select_part += ""
            else:
                table_select_part += ', '

    table_select_part += f" FROM ( SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == f'{epitope_id}':
            table_select_part += f"{epitope_id} "
        elif header == 'num_seq':
            table_select_part += f"count(distinct(seqc.sequence_id)) as {header}"
        elif header == 'num_var':
            table_select_part += f"""sum(CASE
                                         WHEN epic.sequence_id = seqc.sequence_id THEN variant_aa_length
                                         ELSE 0
                                    END) as {header}"""
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            table_select_part += ""
        else:
            table_select_part += f"array_agg(distinct {header}) as {header}"

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop':
                table_select_part += ''
            else:
                table_select_part += ', '

    table_select_part += f" FROM {epitope_table} as epic "

    return table_select_part


def gen_select_epi_query_table2(payload_table_headers):
    table_select_part = f"SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == f'{epitope_id}':
            table_select_part += f"{epitope_id} "
        elif header == 'num_seq':
            table_select_part += f"count(distinct(sequence_id)) as {header}"
        elif header == 'num_var':
            table_select_part += f"sum(variant_aa_length ) as {header}"
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            table_select_part += ""
        else:
            table_select_part += f"array_agg(distinct {header}) as {header}"

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop':
                table_select_part += ''
            else:
                table_select_part += ', '

    table_select_part += f" FROM {epitope_table} as epic"

    return table_select_part


def gen_epitope_part_json_virusviz(epitope_part):

    check = epitope_part.get('epitope_name')

    if check is None:

        epitope_q_id = epitope_part[f'{epitope_id}']
        epitope_query = f"""SELECT {epitope_id},
                                    array_agg(distinct product) as product,
                                    array_agg(distinct row(epi_frag_annotation_start, epi_frag_annotation_stop) 
                                        order by (epi_frag_annotation_start, epi_frag_annotation_stop) ) as all_fragment_position,
                                    array_agg(distinct epitope_iri) as epitope_iri
                                    FROM {epitope_table}
                                    WHERE {epitope_id} = {epitope_q_id}
                                    GROUP BY {epitope_id}"""

        # WITHOUT epi_fragments but takes tooo long

        #epitope_query = f"""SELECT {epitope_id},
        #                    array_agg(distinct product) as product,
        #                    array_agg(distinct row(epif.epi_frag_annotation_start, epif.epi_frag_annotation_stop)
        #                        order by (epif.epi_frag_annotation_start, epif.epi_frag_annotation_stop) ) as all_fragment_position,
        #                    array_agg(distinct epitope_iri) as epitope_iri
        #                    FROM {epitope_table} as epi JOIN epitope_fragment as epif
        #                            ON epif.epitope_id = (SELECT min(epitope_id)
        #                              FROM epitope as c
        #                            WHERE c.iedb_epitope_id = epi.iedb_epitope_id)
        #                    WHERE {epitope_id} = {epitope_q_id}
        #                    GROUP BY {epitope_id}"""

        #                        array_agg(distinct iedb_epitope_id) as iedb_epitope_id

        query = sqlalchemy.text(epitope_query)
        res = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)

        for row in res:
            id = row['iedb_epitope_id']
            link = row['epitope_iri'][0]
            protein = row['product'][0]
            position = []

            all_position = row['all_fragment_position']
            all_position = all_position.replace('{"', '')
            all_position = all_position.replace('"}', '')
            all_position = list(all_position.split('","'))
            length = len(all_position)
            i = 0
            while i < length:
                position_i = all_position[i]
                position_i = position_i.replace('(', '')
                position_i = position_i.replace(')', '')
                position_i = list(position_i.split(','))
                position_single = []
                position_single.append(int(position_i[0]))
                position_single.append(int(position_i[1]))
                i = i + 1
                #if i != length:
                #    position += ","
                position.append(position_single)

        epitope_json = [{
            "id": id,
            "link": link,
            "protein": protein,
            "position": position
        }]

    else:

        name = epitope_part['epitope_name']
        link = epitope_part['link']
        protein_to_query = epitope_part['protein']

        query_protein_name = f"""SELECT distinct product
                                    FROM annotation
                                    WHERE LOWER(product) = '{protein_to_query}'"""

        #                        array_agg(distinct iedb_epitope_id) as iedb_epitope_id

        query_protein = sqlalchemy.text(query_protein_name)
        protein = db.engine.execute(query_protein).fetchall()
        flask.current_app.logger.debug(query_protein)

        all_position = epitope_part['position_range']
        position = []


        all_position = all_position.replace('\n', '')
        all_position = all_position.replace(' ', '')
        all_position = list(all_position.split(','))
        length = len(all_position)
        i = 0
        while i < length:
            position_i = all_position[i]
            position_i = list(position_i.split('-'))
            position_single = []
            position_single.append(int(position_i[0]))
            position_single.append(int(position_i[1]))
            i = i + 1
            position.append(position_single)

        epitope_json = [{
            "id": name,
            "link": link,
            "protein": protein[0][0],
            "position": position
        }]

    return epitope_json


def gen_epitope_part_json_virusviz2(epitope_part):
    epitope_q_id = epitope_part[f'{epitope_id}']
    epitope_query = f"""SELECT {epitope_id},
                        array_agg(distinct product) as product,
                        array_agg(distinct all_fragment_position) as all_fragment_position,
                        array_agg(distinct epitope_iri) as epitope_iri
                        FROM {epitope_table}
                        WHERE {epitope_id} = {epitope_q_id}
                        GROUP BY {epitope_id}"""

    #                        array_agg(distinct iedb_epitope_id) as iedb_epitope_id

    query = sqlalchemy.text(epitope_query)
    res = db.engine.execute(query).fetchall()
    flask.current_app.logger.debug(query)

    for row in res:
        id = row['iedb_epitope_id'][0]
        link = row['epitope_iri'][0]
        protein = row['product'][0]
        position = ""
        all_position = row['all_fragment_position'][0]
        length = len(all_position)
        i = 0
        while i < length:
            position_i = all_position[i]
            position_i = position_i.replace('(', '')
            position_i = position_i.replace(')', '')
            position_i = list(position_i.split(','))
            position += "[" + position_i[2] + "," + position_i[3] + "]"
            i = i + 1
            if i != length:
                position += ","

    epitope_json = [{
        "id": id,
        "link": link,
        "protein": protein,
        "position": "[" + position + "]"
    }]

    return epitope_json


host_taxon_name_dict = {}


def load_hosts():
    from model.models import db

    query = """SELECT distinct host_taxon_name, host_taxon_id
               FROM host_specie"""
    query2 = sqlalchemy.text(query)
    res = db.engine.execute(query2).fetchall()

    for row in res:
        row_dict = dict(row)
        if row_dict['host_taxon_name'] is not None:
            host_taxon_name = row_dict['host_taxon_name'].lower()
        else:
            host_taxon_name = None

        host_taxon_name_dict[host_taxon_name] = row_dict
