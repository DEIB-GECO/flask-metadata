import datetime

import flask
import sqlalchemy
from flask import Response, json
from flask_restplus import Namespace, Resource, fields, inputs

from model.models import db
from .poll import poll_cache

import utils
from utils import sql_query_generator, taxon_name_dict, custom_db_execution, taxon_id_dict

is_gisaid = True
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
    ColumnEpi('Protein Name', 'product', 'Name of the protein where the epitopes must be located', 'str', False, False),
    ColumnEpi('Assay (T/B Cell)', 'cell_type', 'Assay type of the epitopes', 'str', False, False),
    ColumnEpi('HLA restriction', 'mhc_allele', 'HLA restriction that must be related to the epitopes', 'str', False, False),
    ColumnEpi('Linear / Non Linear', 'is_linear', 'Information related to the type of the epitopes (linear or conformational)', 'str', False, False),
    ColumnEpi('Response Freq', 'response_frequency', 'Range in which the response frequency of the epitopes must be included', 'num', False, True),
    ColumnEpi('Position Range', 'position_range', 'Range in which the epitopes must have at least a part included', 'num', True, False),
]

columns_epi_amino = [
    ColumnEpi('Variant Position Range', 'variant_position_range',
              'Range of positions within the amino acid sequence of the gene, based on the reference sequence', 'num',
              True, False),
    ColumnEpi('Variant Type', 'variant_aa_type', 'Type of amino acid change that must appear in the epitopes (SUB = substitution, INS = insertion, DEL = deletion)', 'str', False, False),
    ColumnEpi('Original Aminoacid', 'sequence_aa_original', 'Affected amino acid sequence from the corresponding reference sequence of the chosen Virus', 'str', False, False),
    ColumnEpi('Alternative Aminoacid', 'sequence_aa_alternative', 'Changed amino acid sequence (in the target sequence) with respect to the reference one', 'str', False, False),
]

columns_user_new_epi_sel = [
    ColumnEpi('Epitope Name', 'epitope_name', 'Name of the custom epitope (Names must be unique)', 'str', False, False),
    ColumnEpi('Protein Name', 'product', 'Name of the protein where the custom epitope will be located', 'str', False, False),
    ColumnEpi('Position Range', 'position_range', 'Range within the chosen protein in which the custom epitope is located. More than one range could be selected to create a conformational epitope (Add one fragment range at a time)', 'num', True, False),
]

columns_user_new_epi_amino = [
    ColumnEpi('Protein Name', 'product', 'Protein produced by the sub-sequence within which the amino acid change occurs', 'str', False, False),
    ColumnEpi('Position Range', 'position_range', 'Range of positions within the amino acid sequence of the gene, based on the reference sequence', 'num', True, False),
    ColumnEpi('Variant Type', 'variant_aa_type', 'Type of amino acid change (SUB = substitution, INS = insertion, DEL = deletion)', 'str', False, False),
    ColumnEpi('Original Aminoacid', 'sequence_aa_original', 'Affected amino acid sequence from the corresponding reference sequence of the chosen Virus', 'str', False, False),
    ColumnEpi('Alternative Aminoacid', 'sequence_aa_alternative', 'Changed amino acid sequence (in the target sequence) with respect to the reference one', 'str', False, False),
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

        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

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

        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

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
        print("qui11")
        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

        def async_function():
            try:
                if field_name != 'product':
                    query_ex = """SELECT distinct 
                                    ("""
                    query_ex += field_name
                    query_ex += f""") as label, count(distinct {epitope_id}) as item_count
                                FROM {epitope_table} as epic"""

                    query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                    panel, payload_epi_query, field_name)

                    query_ex += """ group by label
                    order by item_count desc, label asc"""

                if field_name == 'product':
                    query_ex = query_product_all_mat_view(field_name, filter_in, pair_query, type, panel, payload_epi_query)

                print("qui20", query_ex)

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
                print("qui21", e)
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
        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

        def async_function():
            try:
                query_table = ""
                query_table += gen_select_epi_query_table1(payload_table_headers, epitope_table)

                query_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "all")

                query_table += f""" GROUP BY {epitope_id}
                                ) as a"""

                #ORDER BY {epitope_id}

                #query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = a.epitope_id """

                query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = (SELECT min(epitope_id)
												   FROM epitope as c
													WHERE c.iedb_epitope_id = a.iedb_epitope_id)"""

                query_table += group_by_epi_query_table1(payload_table_headers)

                query = sqlalchemy.text(query_table)

                print("qui18", query)
                #res = custom_db_execution(query, poll_id)
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
        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

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
        epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

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

        query_count_variant = """SELECT sum(variant_aa_length) as num_var
                                    FROM ( """

        query_count_variant += sql_query_generator(filter_in, pairs_query=pair_query, search_type=type,
                                return_type="count_variants", field_selected="", panel=panel)

        query_count_variant += """ ) as a"""

        query = sqlalchemy.text(query_count_variant)
        res = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res = [{'count': row['num_var']} for row in res]

        #res =[{'count': 0}]

        return res


@api.route('/epiSelWithoutVariants/<field_name>')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self, field_name):
        payload = api.payload
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        payload_epi_query = payload.get("epi_query")
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_ex = """SELECT distinct 
                                ( """
                if field_name == 'product':
                    query_ex += ' protein_name '
                    #query_ex += ' protein_ncbi_id '
                else:
                    query_ex += field_name

                query_ex += f""" ) as label, count(distinct {epitope_id}) as item_count
                            FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id"""

                query_ex += add_where_epi_query_without_variants(filter_in, payload_epi_query, field_name)

                query_ex += """ group by label
                order by item_count desc, label asc"""

                query_ex_2 = sqlalchemy.text(query_ex)

                print("qui90", query_ex_2)

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
                    if field_name == 'product':
                        if is_gisaid:
                            li = protein_array
                        else:
                            the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
                            taxon_id = the_virus["taxon_id"]
                            all_protein = taxon_id_dict[taxon_id]['a_products']
                            li = [item.get('name') for item in all_protein]
                        list_protein = []
                        for row in res:
                            if row['label'] in li:
                                protein = row['label']
                                count_protein = row['item_count']
                                row_dict = {'label': protein, 'count': count_protein}
                                list_protein.append(row_dict)

                        list_protein.sort(key=lambda s: s['count'], reverse=True)

                        res = [{'value': allele['label'], 'count': allele['count']} for allele in
                               list_protein]

                    else:
                        res = [{'value': row['label'], 'count': row['item_count']} for row in res]

                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                print("qui91", e)
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')



@api.route('/epiFreqExtremesWithoutVariants')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        payload_epi_query = payload.get("epi_query")
        poll_id = poll_cache.create_dict_element()
        field_name = "response_frequency"

        def async_function():
            try:
                query_ex = f"""SELECT min(distinct(epi.response_frequency_pos)) as min,
                                max(distinct(epi.response_frequency_pos)) as max,
                                count(*) as count
                                FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id """

                query_ex += add_where_epi_query_without_variants(filter_in, payload_epi_query, field_name)

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


@api.route('/epiExtremesWithoutVariants/<field_name>')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self, field_name):
        payload = api.payload
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        payload_epi_query = payload.get("epi_query")
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_ex = f"""SELECT min(distinct(epi.epi_annotation_start)) as min, 
                            max(distinct(epi.epi_annotation_stop)) as max
                            FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id """

                query_ex += add_where_epi_query_without_variants(filter_in, payload_epi_query, field_name)

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


@api.route('/countWithoutVariants')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        payload_epi_query = payload.get("epi_query")
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_count_table = f"""SELECT count(distinct {epitope_id}) as count_epi """ \
                                    f"""FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id """

                query_count_table += add_where_epi_query_without_variants(filter_in, payload_epi_query, "all")

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


@api.route('/epiTableResWithoutVariants')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        payload_epi_query = payload.get("epi_query")
        payload_table_headers = payload.get("table_headers")

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                query_table = ""
                query_table += gen_select_epi_query_table_without_variants(payload_table_headers)

                query_table += add_where_epi_query_without_variants(filter_in, payload_epi_query, "all")

                query_table += f""" GROUP BY {epitope_id} """

                query = sqlalchemy.text(query_table)

                #res = custom_db_execution(query, poll_id)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{column: value for column, value in row.items()} for row in res]

                for row in res:
                    for item in row:
                        if item == 'virus_id':
                            the_virus = virus_id_dict[row[item]]
                            taxon_name = the_virus["taxon_name"]
                            row[item] = taxon_name
                        if item == 'host_id':
                            the_host = host_id_dict[row[item]]
                            host_taxon_name = the_host["host_taxon_name"]
                            row[item] = host_taxon_name

                res = {'values': res}

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


@api.route('/proteinCustomEpitope')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        filter_in = payload.get("gcm")
        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                the_virus = virus_dict[filter_in['taxon_name'][0].lower()]
                taxon_id = the_virus["virus_id"]
                the_host = host_dict[filter_in['host_taxon_name'][0].lower()]
                host_taxon_id = the_host["host_id"]

                query_protein = f""" select distinct product 
                                    from annotation as ann 
                                    join sequence as seq on ann.sequence_id = seq.sequence_id
                                    where virus_id = {taxon_id}
                                    and product is not null 
                                    order by product desc"""
                #and is_reference

                query = sqlalchemy.text(query_protein)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{'value': row['product'].lower()} for row in res]

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


@api.route('/allProtein')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload
        filter_in = payload.get("gcm")
        if is_gisaid:
            res = protein_array
        else:
            the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
            taxon_id = the_virus["taxon_id"]
            all_protein = taxon_id_dict[taxon_id]['a_products']
            res = [item.get('name') for item in all_protein]

        return res


############


############ FUNZIONI

def getMatView(item_virus, item):

    name = 'epitope'
    the_virus = taxon_name_dict[item_virus[0].lower()]
    taxon_id = the_virus["taxon_id"]

    item = item[0]
    item = item.lower()
    item = item.replace(' ', '_')
    item = item.replace('-', '_')
    item = item.replace('(', '')
    item = item.replace(')', '')
    item = item.replace("'", '')
    item = item.replace('/', '_')
    item = item.replace('\\', '_')
    item = item[:11]

    name = name + '_' + str(taxon_id) + '_' + item
    res = name
    return res


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

    i = 0

    for (column, values) in payload_epi_query.items():
        if column == f"{epitope_id}":
            where_part += add_and(i, field_name)
            where_part += f" {epitope_id} = {values} "
        elif column == "startExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epi_frag_annotation_stop >= {value} "
        elif column == "stopExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epi_frag_annotation_start <= {value} "
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


def gen_where_epi_query_field_without_variants(payload_epi_query, field_name):

    if field_name == "toTable":
        where_part = " WHERE "
    else:
        where_part = ""

    i = 0

    for (column, values) in payload_epi_query.items():
        if column == f"{epitope_id}":
            where_part += add_and(i, field_name)
            where_part += f" {epitope_id} = {values} "
        elif column == "startExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epif.epi_frag_annotation_stop >= {value} "
        elif column == "stopExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epif.epi_frag_annotation_start <= {value} "
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
        elif column == "product":
            if field_name != "product":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" protein_name = '{value}' "
                    #where_part += f" protein_ncbi_id = '{value}' "
        else:
            col = columns_dict_epi_all[column]
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

    if field_name == "all":
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


def add_where_epi_query_without_variants(filter_in, payload_epi_query, field_name):
    where_part_final = ""

    where_part_final += f" WHERE iedb_epitope_id is not null and "

    the_virus = virus_dict[filter_in['taxon_name'][0].lower()]
    taxon_id = the_virus["virus_id"]
    the_host = host_dict[filter_in['host_taxon_name'][0].lower()]
    host_taxon_id = the_host["host_id"]

    where_part_final += f""" virus_id = {taxon_id} 
                and host_id = {host_taxon_id} """



    if payload_epi_query is not None:
        epi_query_len = len(payload_epi_query)
        if epi_query_len != 0:
            query_where_epi = gen_where_epi_query_field_without_variants(payload_epi_query, field_name)
            where_part_final += query_where_epi

    return where_part_final


def gen_select_epi_query_table(payload_table_headers, epitope_table=None):
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
            table_select_part += f"max({header}) as {header}"

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


def gen_select_epi_query_table1(payload_table_headers, epitope_table):
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
            if header == "is_linear":
                table_select_part += f" bool_and({header}) as {header} "
            elif header == "cell_type":
                table_select_part += f" array_agg(distinct {header}) as {header} "
            else:
                table_select_part += f" max({header}) as {header} "

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop':
                table_select_part += ''
            else:
                table_select_part += ', '

    table_select_part += f" FROM {epitope_table} as epic "

    return table_select_part


def gen_select_epi_query_table_without_variants(payload_table_headers):
    table_select_part = f"SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == 'epi_fragment_sequence':
            table_select_part += f"""array_agg(distinct row(epif2.epi_frag_annotation_start,
                                        epif2.epi_frag_annotation_stop, epif2.{header}) 
                                        order by (epif2.epi_frag_annotation_start,
                                        epif2.epi_frag_annotation_stop, epif2.{header})) as epi_fragment_all_information """
        elif header == f'{epitope_id}':
            table_select_part += f" {header} as {header} "
        elif header == f'is_linear':
            table_select_part += f" bool_and({header}) as {header} "
        elif header == f'cell_type':
            table_select_part += f" array_agg(distinct {header}) as {header} "
        else:
            table_select_part += f" max({header}) as {header} "

        count = count - 1
        if count > 0:
            table_select_part += ', '

    table_select_part += f" FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id join epitope_fragment as epif2 on epi.epitope_id = epif2.epitope_id "

    return table_select_part


def gen_select_epi_query_table2(payload_table_headers, epitope_table=None):
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
            table_select_part += f"max({header}) as {header}"

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop':
                table_select_part += ''
            else:
                table_select_part += ', '

    table_select_part += f" FROM {epitope_table} as epic"

    return table_select_part


def gen_epitope_part_json_virusviz(epitope_part, without_variants=False, all_population=False, filter_in=None):

    if all_population == True:

        epitope_json = []
        i = 0

        while i < len(epitope_part):
            position = []

            all_position = epitope_part[i]['position']
            all_position = all_position.replace('\n', '')
            all_position = list(all_position.split(','))
            length = len(all_position)
            j = 0
            name_start_pos = None
            name_stop_pos = None
            while j < length:
                position_j = all_position[j]
                position_j = list(position_j.split('-'))
                position_single = []
                if position_j[0] != "":
                    position_single.append(int(position_j[0]))
                position_single.append(int(position_j[1]))
                if j == 0:
                    name_start_pos = str(position_single)
                elif j == length - 1:
                    name_stop_pos = str(position_single)
                j = j + 1
                position.append(position_single)

            id = epitope_part[i]['id']
            epitope_part[i]['id'] = name_start_pos
            if name_stop_pos is not None:
                epitope_part[i]['id'] += ' .. ' + name_stop_pos
            epitope_part[i]['id'] += " - " + str(id)
            epitope_part[i]['position'] = position
            i = i + 1

        epitope_part = sorted(epitope_part, key=lambda k: k['position'][0][0], reverse=False)
        epitope_json = epitope_part

    else:

        check = epitope_part.get('epitope_name')

        if check is None:

            if without_variants is True:
                virus_line_dict = virus_dict[filter_in['taxon_name'][0].lower()]
                virus_id = virus_line_dict["virus_id"]

                epitope_q_id = epitope_part[f'{epitope_id}']
                #min(protein_ncbi_id) as product ,
                epitope_query = f"""select {epitope_id}, 
                                    array_agg(protein_name) as product , 
                                    array_agg(epitope_iri) as epitope_iri,
                                    array_agg(distinct row(epi_frag_annotation_start, epi_frag_annotation_stop) 
                                    order by (epi_frag_annotation_start, epi_frag_annotation_stop) ) as all_fragment_position
                                    FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id
                                    WHERE {epitope_id} = {epitope_q_id}
                                    AND virus_id = {virus_id}
                                    group by {epitope_id} """
            else:
                virus_line_dict = virus_dict[filter_in['taxon_name'][0].lower()]
                virus_id = virus_line_dict["virus_id"]
                epitope_q_id = epitope_part[f'{epitope_id}']
                epitope_query = f"""SELECT {epitope_id},
                                            array_agg(distinct protein_name) as product,
                                            array_agg(distinct row(epi_frag_annotation_start, epi_frag_annotation_stop) 
                                                order by (epi_frag_annotation_start, epi_frag_annotation_stop) ) as all_fragment_position,
                                            array_agg(distinct epitope_iri) as epitope_iri
                                            FROM epitope as epi join epitope_fragment as epif on epi.epitope_id = epif.epitope_id
                                            WHERE {epitope_id} = {epitope_q_id}
                                            AND virus_id = {virus_id}
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
            print("qui2019", query)
            res = db.engine.execute(query).fetchall()
            flask.current_app.logger.debug(query)

            for row in res:
                link = row['epitope_iri'][0]
                protein = row['product'][0]
                position = []

                all_position = row['all_fragment_position']
                all_position = all_position.replace('{"', '')
                all_position = all_position.replace('"}', '')
                all_position = list(all_position.split('","'))
                length = len(all_position)
                i = 0
                name_start_pos = None
                name_stop_pos = None
                while i < length:
                    position_i = all_position[i]
                    position_i = position_i.replace('(', '')
                    position_i = position_i.replace(')', '')
                    position_i = list(position_i.split(','))
                    position_single = []
                    position_single.append(int(position_i[0]))
                    position_single.append(int(position_i[1]))
                    if i == 0:
                        name_start_pos = str(position_single)
                    elif i == length - 1:
                        name_stop_pos = str(position_single)
                    i = i + 1
                    #if i != length:
                    #    position += ","
                    position.append(position_single)

                #id = row['iedb_epitope_id']
                id_to_add = row['iedb_epitope_id']
                id = name_start_pos
                if name_stop_pos is not None:
                    id += ' .. ' + name_stop_pos
                id += " - " + str(id_to_add)

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

            virus_line_dict = virus_dict[filter_in['taxon_name'][0].lower()]
            virus_id = virus_line_dict["virus_id"]

            query_protein_name = f"""SELECT distinct product
                                        FROM annotation as ann JOIN sequence as seq on ann.sequence_id = seq.sequence_id
                                        WHERE LOWER(product) = '{protein_to_query}'
                                        AND virus_id = {virus_id}"""

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
            name_start_pos = None
            name_stop_pos = None
            while i < length:
                position_i = all_position[i]
                position_i = list(position_i.split('-'))
                position_single = []
                position_single.append(int(position_i[0]))
                position_single.append(int(position_i[1]))
                if i == 0:
                    name_start_pos = str(position_single)
                elif i == length - 1:
                    name_stop_pos = str(position_single)
                i = i + 1
                position.append(position_single)

            id = name_start_pos
            if name_stop_pos is not None:
                id += ' .. ' + name_stop_pos
            id += " - " + name

            epitope_json = [{
                "id": id,
                "link": link,
                "protein": protein[0][0],
                "position": position
            }]

    return epitope_json


def gen_epitope_part_json_virusviz2(epitope_part):
    epitope_table = None
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


virus_dict = {}
virus_id_dict = {}


def load_virus_id():
    from model.models import db

    query = """SELECT distinct taxon_name, virus_id
               FROM virus"""
    query2 = sqlalchemy.text(query)
    res = db.engine.execute(query2).fetchall()

    for row in res:
        row_dict = dict(row)
        if row_dict['taxon_name'] is not None:
            taxon_name = row_dict['taxon_name'].lower()
            virus_id = row_dict['virus_id']
        else:
            taxon_name = None
            virus_id = None

        virus_dict[taxon_name] = row_dict
        virus_id_dict[virus_id] = row_dict


host_dict = {}
host_id_dict = {}


def load_host_id():
    from model.models import db

    query = """SELECT distinct host_taxon_name, host_id
               FROM host_specie"""
    query2 = sqlalchemy.text(query)
    res = db.engine.execute(query2).fetchall()

    for row in res:
        row_dict = dict(row)
        if row_dict['host_taxon_name'] is not None:
            host_taxon_name = row_dict['host_taxon_name'].lower()
            host_id = row_dict['host_id']
        else:
            host_taxon_name = None
            host_id = None

        host_dict[host_taxon_name] = row_dict
        host_id_dict[host_id] = row_dict


protein_array = []


def load_protein():
    from model.models import db

    query = """SELECT distinct product
               FROM annotation"""
    query2 = sqlalchemy.text(query)
    res = db.engine.execute(query2).fetchall()

    for row in res:
        row_dict = dict(row)
        if row_dict['product'] is not None:
            protein_array.append(row_dict['product'])


def query_product_all_mat_view(field_name, filter_in, pair_query, type, panel, payload_epi_query):

    virus_line_dict = virus_dict[filter_in['taxon_name'][0].lower()]
    virus_id = virus_line_dict["virus_id"]

    query_all_product = f"""SELECT distinct protein_name as product""" \
                        f""" from epitope """ \
                        f""" WHERE virus_id = {virus_id}"""

    query = sqlalchemy.text(query_all_product)
    res = db.engine.execute(query).fetchall()
    flask.current_app.logger.debug(query)

    res = [{'product': row['product']} for row in res]

    all_modified = []

    the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
    taxon_id = the_virus["taxon_id"]

    for product in res:
        item = product['product']
        #item != 'Spike (surface glycoprotein)'
        if item is not None:
            item = item.lower()
            item = item.replace(' ', '_')
            item = item.replace('-', '_')
            item = item.replace('(', '')
            item = item.replace(')', '')
            item = item.replace("'", '')
            item = item.replace('/', '_')
            item = item.replace('\\', '_')
            item = item[:11]
            all_modified.append(item)

    query_product = """"""
    mat_view_example = "epitope"
    i = 0
    for protein in all_modified:
        if i != 0:
            query_product += """ UNION """

        #SELECT label, count(*) as item_count FROM
        query_product += """ SELECT distinct 
                                        ("""
        query_product += field_name

        mat_view = mat_view_example + '_' + str(taxon_id) + '_' + protein
        query_product += f""") as label, count(distinct {epitope_id}) as item_count
                                    FROM {mat_view} as epic"""

        query_product += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                             payload_epi_query, field_name)

        #) as view
        query_product += """ group by label"""

        i = i + 1

    query_product += """ order by item_count desc, label asc"""

    return query_product
