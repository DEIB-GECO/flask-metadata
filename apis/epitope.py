import collections
import datetime

import flask
import sqlalchemy
#import tqdm as tqdm
#import pandas as pd
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
    ColumnEpi('Assay', 'cell_type', 'Assay type of the epitopes', 'str', False, False),
    ColumnEpi('HLA restriction', 'mhc_allele', 'HLA restriction that must be related to the epitopes', 'str', False, False),
    ColumnEpi('Is Linear', 'is_linear', 'Information related to the type of the epitopes (linear or conformational)', 'str', False, False),
    ColumnEpi('Response Frequency', 'response_frequency', 'info resp freq', 'num', False, True),
    ColumnEpi('Position Range', 'position_range', 'Range in which the epitopes must have at least a part included', 'num', True, False),
    ColumnEpi('Epitope IEDB ID', 'iedb_epitope_id', 'IEDB ID of the epitope', 'num', False, False),
    #ColumnEpi('Assay Type', 'assay_type', 'Assay type', 'str', False, False),
]

columns_epi_amino = [
    ColumnEpi('Variant Position Range', 'variant_position_range',
              'Range of positions within the amino acid sequence of the gene, based on the reference sequence. '
              'Insertions and deletions have arbitrary lengths. Substitutions only involve one amino acid residue. '
              'Please search for one single substitution at a time.', 'num',
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


@api.route('/extremesPositionNewEpitopeAminoacid')
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


@api.route('/extremesPositionNewEpitope')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        from . import viz
        payload = api.payload
        product = payload.get('product')
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")

        minPos = 0
        maxPos = 0

        if is_gisaid:
            all_protein = viz.sars_cov_2_products['A']
            for item in all_protein:
                name = str(item.get('name'))
                if name.lower() == product.lower():
                    minPos = 1
                    maxPos = (item.get('end') - item.get('start')) // 3
                    if "nsp" in name.lower():
                        maxPos = maxPos + 1

            res = [{'start': minPos, 'stop': maxPos}]
            res = {'values': res}

        else:
            all_protein = taxon_name_dict[filter_in['taxon_name'][0].lower()]['a_products']
            for item in all_protein:
                name = str(item.get('name'))
                if name.lower() == product.lower():
                    minPos = 1
                    maxPos = (item.get('end') - item.get('start')) // 3
                    if "nsp" in name.lower():
                        maxPos = maxPos + 1

            res = [{'start': minPos, 'stop': maxPos}]
            res = {'values': res}

        return res


@api.route('/sequenceAminoacidNewEpitope')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        from . import viz
        payload = api.payload
        product = payload.get('product')
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        position_ranges = payload.get('position_ranges')

        aa_sequence = ""

        if is_gisaid:
            all_protein = viz.sars_cov_2_products['A']
            for item in all_protein:
                name = str(item.get('name'))
                if name.lower() == product.lower():
                    aa_sequence = item.get('sequence')

        else:
            all_protein = taxon_name_dict[filter_in['taxon_name'][0].lower()]['a_products']
            for item in all_protein:
                name = str(item.get('name'))
                if name.lower() == product.lower():
                    aa_sequence = item.get('sequence')

        position_ranges = position_ranges.replace('\n', '')
        all_position = list(position_ranges.split(','))
        length = len(all_position)
        j = 0
        arrSequences = []
        while j < length:
            position_j = all_position[j]
            position_j = list(position_j.split('-'))

            single_sequence = aa_sequence[int(position_j[0])-1: int(position_j[1])]
            j = j + 1
            arrSequences.append(single_sequence)

        length2 = len(arrSequences)
        k = 0
        res = ""
        while k < length2:
            res += arrSequences[k]
            k = k + 1
            if k != length2:
                res += ",\n"

        return res


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
                    query_ex += f""") as label
                                FROM {epitope_table} as epic"""

                    #, count(distinct {epitope_id}) as item_count

                    query_ex += add_where_epi_query(filter_in, pair_query, type, 'item_id', "",
                                                    panel, payload_epi_query, field_name)

                    query_ex += """ group by label
                    order by label asc"""
                    #item_count desc

                if field_name == 'product':
                    query_ex = query_product_all_mat_view(field_name, filter_in, pair_query, type, panel, payload_epi_query)

                print("qui20", query_ex)

                query_ex_2 = sqlalchemy.text(query_ex)

                res = db.engine.execute(query_ex_2).fetchall()
                flask.current_app.logger.debug(query_ex_2)

                #if field_name == 'mhc_allele':
                #    list_separated_mhc_allele = []
                #    for row in res:
                #        all_mhc_allele = row['label']
                #        count_mhc_allele = row['item_count']
                #        if all_mhc_allele is not None:
                #            list_mhc_allele = list(all_mhc_allele.split(','))
                #            for item in list_mhc_allele:
                #                row_dict = {'label': item, 'count': count_mhc_allele}
                #                if not any(allele['label'] == item for allele in list_separated_mhc_allele):
                #                    list_separated_mhc_allele.append(row_dict)
                #                else:
                #                    for allele in list_separated_mhc_allele:
                #                        if allele['label'] == item:
                #                            allele['count'] = allele['count'] + count_mhc_allele
                #        else:
                #            list_mhc_allele = None
                #            row_dict = {'label': list_mhc_allele, 'count': count_mhc_allele}
                #            list_separated_mhc_allele.append(row_dict)
                #
                #    list_separated_mhc_allele.sort(key=lambda s: s['count'], reverse=True)
                #
                #    res = [{'value': allele['label'], 'count': allele['count']} for allele in list_separated_mhc_allele]
                #else:
                #    res = [{'value': row['label'], 'count': row['item_count']} for row in res]
                res = [{'value': row['label']} for row in res]
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

                query_table += f""" GROUP BY epic.{epitope_id}
                                ) as a"""

                #ORDER BY {epitope_id}

                #query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = a.epitope_id """

                query_table += """ JOIN epitope_fragment as epif ON epif.epitope_id = (SELECT min(epitope_id)
												   FROM epitope as c
													WHERE c.iedb_epitope_id = a.iedb_epitope_id)
									 JOIN epitope as epiepi on epiepi.iedb_epitope_id = a.iedb_epitope_id """

                query_table += group_by_epi_query_table1(payload_table_headers)

                query = sqlalchemy.text(query_table)

                print("qui18", query)
                #res = custom_db_execution(query, poll_id)
                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{column: value for column, value in row.items()} for row in res]

                res2 = []
                for row in res:
                    new_row = row.copy()
                    cell_type = []
                    mhc_allele = []
                    response_frequency_pos = []
                    mhc_class = []
                    assay_type = []
                    for item in row:
                        if item == 'all_array_info':
                            to_iterate = row[item]
                            for subitem in to_iterate:
                                cell_type.append(subitem[0])
                                mhc_allele.append(subitem[1])
                                response_frequency_pos.append(subitem[2])
                                mhc_class.append(subitem[3])
                                assay_type.append(subitem[4])
                                new_row['cell_type'] = cell_type
                                new_row['mhc_allele'] = mhc_allele
                                new_row['response_frequency_pos'] = response_frequency_pos
                                new_row['mhc_class'] = mhc_class
                                new_row['assay_type'] = assay_type
                    res2.append(new_row)

                res = {'values': res2}

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
                query_count_table = f"SELECT count(distinct epic.{epitope_id}) as count_epi FROM {epitope_table} as epic"

                query_count_table += add_where_epi_query(filter_in, pair_query, type, 'item_id', "", panel,
                                                   payload_epi_query, "")

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

        query_count_variant = """SELECT count(variant_aa_length) as num_var
                                    FROM ( """
                            #"""SELECT sum(variant_aa_length) as num_var

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

                res2 = []
                for row in res:
                    new_row = row.copy()
                    cell_type = []
                    mhc_allele = []
                    response_frequency_pos = []
                    mhc_class = []
                    assay_type = []
                    for item in row:
                        if item == 'virus_id':
                            the_virus = virus_id_dict[row[item]]
                            taxon_name = the_virus["taxon_name"]
                            new_row[item] = taxon_name
                        if item == 'host_id':
                            the_host = host_id_dict[row[item]]
                            host_taxon_name = the_host["host_taxon_name"]
                            new_row[item] = host_taxon_name
                        if item == 'all_array_info':
                            to_iterate = row[item]
                            for subitem in to_iterate:
                                cell_type.append(subitem[0])
                                mhc_allele.append(subitem[1])
                                response_frequency_pos.append(subitem[2])
                                mhc_class.append(subitem[3])
                                assay_type.append(subitem[4])
                                new_row['cell_type'] = cell_type
                                new_row['mhc_allele'] = mhc_allele
                                new_row['response_frequency_pos'] = response_frequency_pos
                                new_row['mhc_class'] = mhc_class
                                new_row['assay_type'] = assay_type
                    res2.append(new_row)

                res = {'values': res2}

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
                                    and is_reference
                                    and product is not null 
                                    order by product desc"""

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


@api.route('/totalMutationStatistics')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):
        payload = api.payload

        epitope_id = payload.get("epitopeID")
        payload_epi_query = payload.get("epi_query")
        payload_cmp_query = payload.get("compound_query")
        filter_in = payload_cmp_query.get("gcm")
        type = payload_cmp_query.get("type")
        pair_query = payload_cmp_query.get("kv")
        panel = payload_cmp_query.get("panel")
        payload_parameters = payload.get("parameters")
        first_parameter = payload_parameters.get("firstParameter")
        second_parameter = payload_parameters.get("secondParameter")

        if epitope_id is not None:
            epitope_table = getMatView(filter_in['taxon_name'], payload_epi_query['product'])

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:

                first_select_part = ""
                first_select_part += " SELECT "
                for item in [first_parameter, second_parameter]:
                    if item is not None:
                        if item == "Collection date as month":
                            first_select_part += " col_date "
                        elif item == "Collection date as year":
                            first_select_part += " col_date "
                        elif item == "Lineage":
                            first_select_part += " lineage "
                        elif item == "Clade":
                            first_select_part += " clade "
                        elif item == "Country":
                            first_select_part += " country "
                        elif item == "Region":
                            first_select_part += " region "
                        elif item == "Continent":
                            first_select_part += " geo_group "
                        first_select_part += ", "
                first_select_part += " array_agg((start_aa_original, sequence_aa_original, " \
                                     "sequence_aa_alternative, num_var) order by (start_aa_original, " \
                                     "sequence_aa_original, sequence_aa_alternative, num_var)) as all_info "

                first_select_part += " FROM ( "

                second_select_part = ""
                second_select_part += " SELECT "
                for item in [first_parameter, second_parameter]:
                    if item is not None:
                        if item == "Collection date as month":
                            second_select_part += " TO_DATE(REPLACE(collection_date, '-', '/'), 'YY-MM')::text as col_date "
                        elif item == "Collection date as year":
                            second_select_part += " TO_DATE(REPLACE(collection_date, '-', '/'), 'YY')::text as col_date "
                        elif item == "Lineage":
                            second_select_part += " lineage "
                        elif item == "Clade":
                            second_select_part += " clade "
                        elif item == "Country":
                            second_select_part += " country "
                        elif item == "Region":
                            second_select_part += " region "
                        elif item == "Continent":
                            second_select_part += " geo_group "
                        second_select_part += ", "

                if epitope_id is not None:
                    second_select_part += " start_aa_original, sequence_aa_original, sequence_aa_alternative, " \
                                       "count(*) / count(DISTINCT epic.cell_type) as num_var "
                else:
                    second_select_part += " start_aa_original, sequence_aa_original, sequence_aa_alternative, " \
                                          "count(*) as num_var "

                if epitope_id is not None:
                    second_select_part += f" FROM {epitope_table} AS epic "

                    where_part_final = ""
                    where_part_final += f""" JOIN ( """
                    query_seq_sel = sql_query_generator(filter_in, pairs_query=pair_query, search_type=type,
                                                        return_type="allInfo", field_selected="", panel=panel)
                    where_part_final += query_seq_sel
                    where_part_final += f""" ) as seqc ON epic.sequence_id = seqc.sequence_id """


                    the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
                    taxon_id = the_virus["taxon_id"]
                    the_host = host_taxon_name_dict[filter_in['host_taxon_name'][0].lower()]
                    host_taxon_id = the_host["host_taxon_id"]

                    where_part_epitope = " WHERE "
                    where_part_epitope += f""" epic.taxon_id = {taxon_id} 
                                    and epic.host_taxon_id = {host_taxon_id} 
                                    and epic.iedb_epitope_id = {epitope_id}"""
                else:
                    second_select_part += f" FROM ( "
                    where_part_final = ""
                    query_seq_sel = sql_query_generator(filter_in, pairs_query=pair_query, search_type=type,
                                                        return_type="allInfoCustomEpi", field_selected="", panel=panel)
                    where_part_final += query_seq_sel
                    where_part_final += " ) as A "


                group_by_part = " GROUP BY "
                for item in [first_parameter, second_parameter]:
                    if item is not None:
                        if item == "Collection date as month":
                            group_by_part += " col_date "
                        elif item == "Collection date as year":
                            group_by_part += " col_date "
                        elif item == "Lineage":
                            group_by_part += " lineage "
                        elif item == "Clade":
                            group_by_part += " clade "
                        elif item == "Country":
                            group_by_part += " country "
                        elif item == "Region":
                            group_by_part += " region "
                        elif item == "Continent":
                            group_by_part += " geo_group "
                        group_by_part += ", "

                group_by_part += " start_aa_original, sequence_aa_original, sequence_aa_alternative ) as b "

                group_by_part_2 = ""
                i = 0
                for item in [first_parameter, second_parameter]:
                    if item is not None:
                        if i == 0:
                            group_by_part_2 += " GROUP BY "
                        if i > 0:
                            group_by_part_2 += " , "
                        if item == "Collection date as month":
                            group_by_part_2 += " col_date "
                        elif item == "Collection date as year":
                            group_by_part_2 += " col_date "
                        elif item == "Lineage":
                            group_by_part_2 += " lineage "
                        elif item == "Clade":
                            group_by_part_2 += " clade "
                        elif item == "Country":
                            group_by_part_2 += " country "
                        elif item == "Region":
                            group_by_part_2 += " region "
                        elif item == "Continent":
                            group_by_part_2 += " geo_group "
                        i = i + 1

                query_table = first_select_part + second_select_part + where_part_final
                if epitope_id is not None:
                    query_table += where_part_epitope

                query_table += group_by_part + group_by_part_2

                query = sqlalchemy.text(query_table)

                res = db.engine.execute(query).fetchall()
                flask.current_app.logger.debug(query)

                res = [{column: value for column, value in row.items()} for row in res]
                res = {'values': res}

                #return res

                poll_cache.set_result(poll_id, res)
            except Exception as e:
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')


######################################## API NEW PROJECT ############################################################


@api.route('/statisticsMutationsLineages')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        list_pairs = api.payload

        lineage_stats = {}

        if list_pairs is None:
            list_pairs = [('c101', 'B.1'),
                          ('c103', 'B.1.243')]

        #for l in tqdm.tqdm(set([l for c, l in list_pairs])):
        for l in set([l for c, l in list_pairs]):
            query = f"""
            SELECT sequence_id,  product, sequence_aa_original, start_aa_original, sequence_aa_alternative
            FROM sequence
            NATURAL JOIN annotation
            NATURAL JOIN aminoacid_variant
             WHERE lineage ilike '{l}'
             AND  product = 'Spike (surface glycoprotein)'
            ORDER BY sequence_id, product, start_aa_original

            """

            rows = db.engine.execute(query).fetchall()
            ln = len(set([x[0] for x in rows]))

            lineage_stats[l] = {x: y / ln for x, y in
                            collections.Counter([(x[-2], x[-3], x[-1]) for x in rows]).items()}

        return lineage_stats


@api.route('/mutationForSequence')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        id_sequence = api.payload

        query = f"""
        select product, lineage, array_agg(row(start_aa_original, sequence_aa_original, sequence_aa_alternative))
        from sequence as it JOIN annotation as ann on ann.sequence_id = it.sequence_id
        JOIN aminoacid_variant as amin on ann.annotation_id = amin.annotation_id
        where accession_id = '{id_sequence}' and product like '%%Spike%%'
        group by product, lineage
        """

        res = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        print("q", res)
        res = [{column: value for column, value in row.items()} for row in res]

        return res


@api.route('/statisticsMutationsLineagesGET')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def get(self):

        empty = {}
        if lineage_stats_dict == empty:
            print("lin stats empty")

            query_lin_count = f"""
            SELECT lineage, count(*) as total
            FROM sequence
            WHERE virus_id = 1
            GROUP BY lineage
            """

            res_lin_count = db.engine.execute(query_lin_count).fetchall()
            flask.current_app.logger.debug(query_lin_count)

            lin_dict = {}
            for row in res_lin_count:
                row_dict = dict(row)
                if row_dict['lineage'] is not None:
                    lineage = row_dict['lineage']
                else:
                    lineage = None
                lin_dict[lineage] = row_dict

            # res_lin_count = [{column: value for column, value in row.items()} for row in res_lin_count]

            query_all = f"""
            SELECT lineage, product, sequence_aa_original, start_aa_original, sequence_aa_alternative, count(*) as total
            FROM sequence
            NATURAL JOIN annotation
            NATURAL JOIN aminoacid_variant
            WHERE virus_id = 1
            GROUP BY lineage, product, sequence_aa_original, start_aa_original, sequence_aa_alternative
            ORDER BY start_aa_original asc
            """

            res_all = db.engine.execute(query_all).fetchall()
            flask.current_app.logger.debug(query_all)

            res_all = [{column: value for column, value in row.items()} for row in res_all]

            lineage_stats = {}
            for row in res_all:
                for item in row:
                    new_row = {}
                    if row['lineage'] in lineage_stats:
                        if item == "total":
                            line = lineage_stats[row['lineage']]
                            name = (row['start_aa_original'], row['sequence_aa_original'], row['sequence_aa_alternative'], row['product'])
                            denominator = lin_dict[row['lineage']]
                            line[name] = row[item] / denominator['total']
                    else:
                        if item == "total":
                            name = (row['start_aa_original'], row['sequence_aa_original'], row['sequence_aa_alternative'], row['product'])
                            denominator = lin_dict[row['lineage']]
                            new_row[name] = row[item] / denominator['total']
                        lineage_stats[row['lineage']] = new_row

            return lineage_stats
        else:
            print("lin stats NOT empty")
            return lineage_stats_dict


@api.route('/tableLineageCountry')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        filter_geo = api.payload     # {'type': 'country', 'value': 'Italy', 'minCountSeq': 500}
        # filter_geo = {'type': 'country', 'value': 'Italy', 'minCountSeq': 500}
        geo_selection = 'country'
        geo_min_count = filter_geo['minCountSeq']
        geo_where = filter_geo['type']
        min_date = filter_geo['minDate']
        max_date = filter_geo['maxDate']
        geo_where_value = filter_geo['value']
        geo_where_value = geo_where_value.replace("'", "''")

        if geo_where_value is not None:
            geo_where_value = geo_where_value.lower()

        if geo_where == 'geo_group':
            geo_selection = 'country'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'country':
            geo_selection = 'region'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'region':
            geo_selection = 'province'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'world':
            geo_selection = 'geo_group'
            geo_where_part = f""" """

        query = f"""SELECT a.lineage, array_agg(row(REPLACE(a.{geo_selection}, ',', ' -'), a.cnt)) as country_count
                    FROM (
                    SELECT lineage, {geo_selection}, count(*) as cnt
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id """
        query += f""" WHERE collection_date >= '{min_date}'
                      AND collection_date <= '{max_date}'
                      AND coll_date_precision > 1
                       {geo_where_part}"""
        query += f"""GROUP BY lineage, {geo_selection}
                    ORDER BY lineage) as a
                    GROUP BY a.lineage
                    HAVING (sum(a.cnt)/ (SELECT count(distinct it2.sequence_id)
                             FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                             WHERE collection_date >= '{min_date}' 
                             AND collection_date <= '{max_date}'
                             AND coll_date_precision > 1
                             {geo_where_part}
                            )
                   )*100 >= {geo_min_count}"""

        res_all = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res_all = [{column: value for column, value in row.items()} for row in res_all]

        return res_all


@api.route('/denominatorLineageCountry')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        filter_geo = api.payload

        geo_selection = 'country'
        geo_where = filter_geo['type']
        min_date = filter_geo['minDate']
        max_date = filter_geo['maxDate']
        geo_where_value = filter_geo['value']
        geo_where_value = geo_where_value.replace("'", "''")

        if geo_where_value is not None:
            geo_where_value = geo_where_value.lower()

        if geo_where == 'geo_group':
            geo_selection = 'country'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'country':
            geo_selection = 'region'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'region':
            geo_selection = 'province'
            geo_where_part = f""" AND LOWER({geo_where}) = '{geo_where_value}' """
        elif geo_where == 'world':
            geo_selection = 'geo_group'
            geo_where_part = f""" """

        query = f""" SELECT REPLACE({geo_selection}, ',', ' -') as geo, count(*) as cnt
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                    WHERE collection_date >= '{min_date}'
                    AND collection_date <= '{max_date}'
                    AND coll_date_precision > 1
                    {geo_where_part}
                    GROUP BY {geo_selection} """

        res_all = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res_all = [{column: value for column, value in row.items()} for row in res_all]

        return res_all


@api.route('/arrayCountryForLineage')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        lineage = payload['lineage']     # lineage = 'B.1'
        min_count = payload['min_count']     # min_count = 5

        query_country = f"""SELECT a.country, a.cnt, a.total
                     FROM (
                     SELECT lineage, country, count(*) as cnt, (SELECT count(distinct it2.sequence_id)
                               FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                               WHERE hs.country = hs2.country
                               AND hs2.coll_date_precision > 1
                             ) as total
				     FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                     WHERE lineage = '{lineage}'
                     AND hs.coll_date_precision > 1
                     GROUP BY lineage, country) as a
                     GROUP BY a.country, a.cnt, a.total
                     ORDER BY a.country asc"""

        res_country = db.engine.execute(query_country).fetchall()
        flask.current_app.logger.debug(query_country)
        array_country = []
        res_all = [{column: value for column, value in row.items()} for row in res_country]

        for row in res_all:
            if (row['cnt']/row['total'])*100 > min_count:
                array_country.append(row['country'])

        return array_country


@api.route('/analyzeMutationCountryLineage')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        lineage = payload['lineage']    # 'B.1'
        array_country = payload['country']    # ['Italy']
        array_protein = payload['protein']  # ['Spike (surface glycoprotein)']

        array_result = []
        for country in array_country:
            where_protein = ""
            k = 0
            length = len(array_protein)
            for protein in array_protein:
                protein = protein.replace("'", "''")
                if k == 0:
                    where_protein += f""" AND (product = '{protein}' """
                else:
                    where_protein += f""" OR product = '{protein}' """
                k = k + 1
                if k == length:
                    where_protein += """ ) """

            country_to_send = country.replace("'", "''")
            query1 = f""" SELECT distinct ann.product, start_aa_original, sequence_aa_original,
                            sequence_aa_alternative, count(*) as total
                            FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                            JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                            JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                            WHERE lineage = '{lineage}' AND country = '{country_to_send}'
                            AND coll_date_precision > 1
                            {where_protein}
                            GROUP BY ann.product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                            ORDER BY product, start_aa_original """

            res_query1 = db.engine.execute(query1).fetchall()
            flask.current_app.logger.debug(query1)
            res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

            query_count_denominator = f""" SELECT count(*)
                                FROM 
                                (
                                    SELECT distinct it.sequence_id
                                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                    WHERE lineage = '{lineage}' AND country != '{country_to_send}'
                                    AND coll_date_precision > 1
                                ) as a"""

            res_query_count_denominator = db.engine.execute(query_count_denominator).fetchall()
            flask.current_app.logger.debug(query_count_denominator)
            res_query_count_denominator = [{column: value for column, value in row.items()}
                                           for row in res_query_count_denominator]

            denominator = res_query_count_denominator[0]['count']

            query_count_denominator_country = f""" SELECT count(*)
                                            FROM 
                                            (
                                                SELECT distinct it.sequence_id
                                                FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                                WHERE lineage = '{lineage}' AND country = '{country_to_send}'
                                                AND coll_date_precision > 1
                                            ) as a"""

            res_query_count_denominator_country = db.engine.execute(query_count_denominator_country).fetchall()
            flask.current_app.logger.debug(query_count_denominator_country)
            res_query_count_denominator_country = [{column: value for column, value in row.items()}
                                           for row in res_query_count_denominator_country]

            denominator_country = res_query_count_denominator_country[0]['count']

            query_background = f"""SELECT product, start_aa_original, sequence_aa_original, 
                                            sequence_aa_alternative, count(*) as total
                                            FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                            JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                                            JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                                            WHERE lineage = '{lineage}' AND country != '{country_to_send}'
                                            AND coll_date_precision > 1
                                            {where_protein}
                                            GROUP BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                                            ORDER BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative"""

            res_query_background = db.engine.execute(query_background).fetchall()
            flask.current_app.logger.debug(query_background)
            res_query_background = [{column: value for column, value in row.items()}
                                    for row in res_query_background]

            for item in res_query1:
                numerator = 0
                for item2 in res_query_background:
                    if item['start_aa_original'] == item2['start_aa_original'] \
                            and item['sequence_aa_original'] == item2['sequence_aa_original'] \
                            and item['sequence_aa_alternative'] == item2['sequence_aa_alternative'] \
                            and item['product'] == item2['product']:
                        numerator = item2['total']

                if denominator == 0:
                    fraction = 0
                else:
                    fraction = (numerator / denominator)
                if denominator_country == 0:
                    fraction_country = 0
                else:
                    fraction_country = (item['total'] / denominator_country)

                single_line = {'lineage': lineage, 'country': country, 'count_seq': item['total'],
                               'start_aa_original': item['start_aa_original'],
                               'product': item['product'],
                               'sequence_aa_original': item['sequence_aa_original'],
                               'sequence_aa_alternative': item['sequence_aa_alternative'],
                               'numerator': numerator,
                               'denominator': denominator,
                               'fraction': fraction * 100,
                               'denominator_country': denominator_country,
                               'fraction_country': fraction_country * 100}

                array_result.append(single_line)

        return array_result


@api.route('/analyzeMutationCountryLineageInTime')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        start_target_time = payload['start_target']    # '2021-03-31'
        end_target_time = payload['end_target']        # '2021-06-31'
        start_background_time = payload['start_background']  # '2019-01-31'
        end_background_time = payload['end_background']      # '2021-03-31'
        array_protein = payload['protein']                 # ['Spike (surface glycoprotein)']

        query_fields = payload['query']

        array_result = []

        where_protein = ""
        k = 0
        length = len(array_protein)
        for protein in array_protein:
            protein = protein.replace("'", "''")
            if k == 0:
                where_protein += f""" AND (product = '{protein}' """
            else:
                where_protein += f""" OR product = '{protein}' """
            k = k + 1
            if k == length:
                where_protein += """ ) """

        i = 0
        where_part = ""
        if query_fields is not None:
            for key in query_fields:
                if key == 'minDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            j = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part += f""" WHERE """
                                else:
                                    where_part += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        where_part += f""" AND """
                        replace_fields_value = query_fields[key].replace("'", "''")
                        where_part += f""" {key} = '{replace_fields_value}' """
                i = i + 1

        if 'lineage' in query_fields:
            lineage = query_fields['lineage']
        else:
            lineage = 'empty'
        if 'province' in query_fields:
            geo1 = query_fields['province']
        elif 'region' in query_fields:
            geo1 = query_fields['region']
        elif 'country' in query_fields:
            geo1 = query_fields['country']
        elif 'geo_group' in query_fields:
            geo1 = query_fields['geo_group']
        else:
            geo1 = 'empty'

        query1 = f"""  SELECT distinct ann.product, start_aa_original, sequence_aa_original,
                sequence_aa_alternative, count(*) as total
                FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                WHERE collection_date >= '{start_target_time}'
                AND collection_date <= '{end_target_time}'
                AND coll_date_precision > 1
                {where_part}
                {where_protein}
                GROUP BY ann.product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                ORDER BY product, start_aa_original  """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        query_count_denominator = f""" SELECT count(*)
                        FROM 
                        (
                            SELECT distinct it.sequence_id
                            FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                            WHERE collection_date <= '{end_background_time}'
                            AND collection_date >= '{start_background_time}'
                            AND coll_date_precision > 1
                            {where_part}
                        ) as a """

        res_query_count_denominator = db.engine.execute(query_count_denominator).fetchall()
        flask.current_app.logger.debug(query_count_denominator)
        res_query_count_denominator = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator]

        denominator = res_query_count_denominator[0]['count']

        query_count_denominator_target = f""" SELECT count(*)
                            FROM 
                            (
                                SELECT distinct it.sequence_id
                                FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                WHERE collection_date >= '{start_target_time}'
                                AND collection_date <= '{end_target_time}'
                                AND coll_date_precision > 1
                                {where_part}
                            ) as a """

        res_query_count_denominator_target = db.engine.execute(query_count_denominator_target).fetchall()
        flask.current_app.logger.debug(query_count_denominator_target)
        res_query_count_denominator_target = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator_target]

        denominator_country = res_query_count_denominator_target[0]['count']

        query_background = f"""SELECT product, start_aa_original, sequence_aa_original, 
                                                    sequence_aa_alternative, count(*) as total
                                                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                                    JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                                                    JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                                                    WHERE collection_date <= '{end_background_time}'
                                                    AND collection_date >= '{start_background_time}'
                                                    AND coll_date_precision > 1
                                                    {where_part}
                                                    {where_protein}
                                                    GROUP BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                                                    ORDER BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative"""

        res_query_background = db.engine.execute(query_background).fetchall()
        flask.current_app.logger.debug(query_background)
        res_query_background = [{column: value for column, value in row.items()}
                                for row in res_query_background]

        for item in res_query1:
            numerator = 0
            for item2 in res_query_background:
                if item['start_aa_original'] == item2['start_aa_original'] \
                        and item['sequence_aa_original'] == item2['sequence_aa_original'] \
                        and item['sequence_aa_alternative'] == item2['sequence_aa_alternative'] \
                        and item['product'] == item2['product']:
                    numerator = item2['total']

            if denominator == 0:
                fraction = 0
            else:
                fraction = (numerator / denominator)
            if denominator_country == 0:
                fraction_target = 0
            else:
                fraction_target = (item['total'] / denominator_country)

            single_line = {'lineage': lineage, 'country': geo1, 'count_seq': item['total'],
                           'target_time': start_target_time + '//' + end_target_time,
                           'background_time': start_background_time + '//' + end_background_time,
                           'start_aa_original': item['start_aa_original'],
                           'product': item['product'],
                           'sequence_aa_original': item['sequence_aa_original'],
                           'sequence_aa_alternative': item['sequence_aa_alternative'],
                           'numerator': numerator,
                           'denominator': denominator,
                           'fraction': fraction*100,
                           'denominator_target': denominator_country,
                           'fraction_target': fraction_target*100}

            array_result.append(single_line)

        return array_result


@api.route('/analyzeTimeDistributionCountryLineage')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        query_fields = payload['query']

        i = 0
        where_part = ""
        if query_fields is not None:
            for key in query_fields:
                if key == 'minDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            j = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part += f""" WHERE """
                                else:
                                    where_part += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        where_part += f""" AND """
                        replace_fields_value = query_fields[key].replace("'", "''")
                        where_part += f""" {key} = '{replace_fields_value}' """
                i = i + 1

        query1 = f""" SELECT collection_date as name, count(*) as value
                FROM sequence as it JOIN host_sample as hs ON hs.host_sample_id = it.host_sample_id
                WHERE collection_date > '2019-01-01'
                AND coll_date_precision > 1
                {where_part}
                GROUP BY collection_date
                ORDER BY collection_date """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        return res_query1


@api.route('/analyzeTimeDistributionBackgroundQueryGeo')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        query_fields = payload['query']
        query_false = payload['query_false']

        i = 0
        where_part = ""
        if query_fields is not None:
            for key in query_fields:
                if key == 'minDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDate':
                    where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            j = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part += f""" WHERE """
                                else:
                                    where_part += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        where_part += f""" AND """
                        replace_fields_value = query_fields[key].replace("'", "''")
                        if key == query_false:
                            where_part += f""" ( {key} != '{replace_fields_value}' OR
                                                                        {key} is null ) """
                        else:
                            where_part += f""" {key} = '{replace_fields_value}' """
                i = i + 1

        query1 = f""" SELECT collection_date as name, count(*) as value
                FROM sequence as it JOIN host_sample as hs ON hs.host_sample_id = it.host_sample_id
                WHERE collection_date > '2019-01-01'
                AND coll_date_precision > 1
                {where_part}
                GROUP BY collection_date
                ORDER BY collection_date """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        return res_query1


@api.route('/analyzeMutationProvinceRegion')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        # type_geo1 = payload['type_geo1']      # type_geo1 = 'country'
        # geo1 = payload['geo1']                # geo1 = 'Italy'
        # type_geo2 = payload['type_geo2']      # type_geo2 = 'region'
        # geo2 = payload['geo2']                # geo2 = 'Campania'

        # test_lineage = """ AND ( lineage = 'B.1.1.7'
        #                    OR lineage = 'B.1.318'
        #                    OR lineage = 'B.1.351'
        #                    OR lineage = 'B.1.525'
        #                    OR lineage = 'B.1.526'
        #                    OR lineage = 'B.1.526.1'
        #                    OR lineage = 'C.37'
        #                    OR lineage = 'P.1'
        #                    OR lineage = 'P.1.2' ) """

        array_protein = payload['protein']    # ['Spike (surface glycoprotein)']
        query_fields = payload['query']
        toExcludeBackground = payload['toExcludeBackground']

        if 'province' in query_fields:
            target = query_fields['province']
            target_key = 'province'
            if 'region' in query_fields:
                background = query_fields['region']
            elif 'country' in query_fields:
                background = query_fields['country']
            elif 'geo_group' in query_fields:
                background = query_fields['geo_group']
            else:
                background = 'World'
        elif 'region' in query_fields:
            target = query_fields['region']
            target_key = 'region'
            if 'country' in query_fields:
                background = query_fields['country']
            elif 'geo_group' in query_fields:
                background = query_fields['geo_group']
            else:
                background = 'World'
        elif 'country' in query_fields:
            target = query_fields['country']
            target_key = 'country'
            if 'geo_group' in query_fields:
                background = query_fields['geo_group']
            else:
                background = 'World'
        elif 'geo_group' in query_fields:
            target = query_fields['geo_group']
            target_key = 'geo_group'
            background = 'World'
        else:
            target = 'empty'
            target_key = 'empty'
            background = 'empty'

        if 'lineage' in query_fields:
            lineage = query_fields['lineage']
        else:
            lineage = 'empty'

        i = 0
        where_part_target = ""
        where_part_background = ""
        if query_fields is not None:
            for key in query_fields:
                if i == 0:
                    where_part_target += f""" WHERE """
                    where_part_background += f""" WHERE """
                else:
                    where_part_target += f""" AND """
                    where_part_background += f""" AND """
                if key == 'minDate':
                    where_part_target += f""" collection_date >= '{query_fields[key]}' """
                    where_part_background += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDate':
                    where_part_target += f""" collection_date <= '{query_fields[key]}' """
                    where_part_background += f""" collection_date <= '{query_fields[key]}' """
                else:
                    replace_fields_value = query_fields[key].replace("'", "''")
                    if key == target_key:
                        where_part_target += f""" {key} = '{replace_fields_value}' """
                        where_part_background += f""" ( {key} != '{replace_fields_value}' OR
                                                        {key} is null ) """
                    else:
                        where_part_target += f""" {key} = '{replace_fields_value}' """
                        where_part_background += f""" {key} = '{replace_fields_value}' """

                i = i + 1

            for fieldToExclude in toExcludeBackground:
                for geoToExclude in toExcludeBackground[fieldToExclude]:
                    geo_value = geoToExclude.replace("'", "''")
                    where_part_background += f""" AND {fieldToExclude} != '{geo_value}' """

        where_protein = ""
        k = 0
        length = len(array_protein)
        for protein in array_protein:
            protein = protein.replace("'", "''")
            if k == 0:
                where_protein += f""" AND (product = '{protein}' """
            else:
                where_protein += f""" OR product = '{protein}' """
            k = k + 1
            if k == length:
                where_protein += """ ) """

        array_result = []
        # geo1 = geo1.replace("'", "''")
        # geo2 = geo2.replace("'", "''")
        query1 = f""" SELECT distinct ann.product, start_aa_original, sequence_aa_original,
                        sequence_aa_alternative, count(*) as total, array_agg(distinct lineage) as lineage
                        FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                        JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                        JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                        {where_part_target}
                        AND coll_date_precision > 1
                        {where_protein}
                        GROUP BY ann.product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                        ORDER BY product, start_aa_original """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        query_count_denominator = f""" SELECT count(*)
                        FROM 
                        (
                            SELECT distinct it.sequence_id
                            FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                            {where_part_background}
                            AND coll_date_precision > 1
                        ) as a """

        res_query_count_denominator = db.engine.execute(query_count_denominator).fetchall()
        flask.current_app.logger.debug(query_count_denominator)
        res_query_count_denominator = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator]

        denominator = res_query_count_denominator[0]['count']

        query_count_denominator_target = f""" SELECT count(*)
                                FROM 
                                (
                                    SELECT distinct it.sequence_id
                                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                    {where_part_target}
                                    AND coll_date_precision > 1
                                ) as a """

        res_query_count_denominator_target = db.engine.execute(query_count_denominator_target).fetchall()
        flask.current_app.logger.debug(query_count_denominator_target)
        res_query_count_denominator_target = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator_target]

        denominator_target = res_query_count_denominator_target[0]['count']

        query_background = f""" SELECT product, start_aa_original, sequence_aa_original, 
                                sequence_aa_alternative, count(*) as total
                                FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                                JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                                {where_part_background}
                                AND coll_date_precision > 1
                                {where_protein}
                                GROUP BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                                ORDER BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative"""

        res_query_background = db.engine.execute(query_background).fetchall()
        flask.current_app.logger.debug(query_background)
        res_query_background = [{column: value for column, value in row.items()}
                                for row in res_query_background]

        for item in res_query1:
            numerator = 0
            for item2 in res_query_background:
                if item['start_aa_original'] == item2['start_aa_original'] \
                        and item['sequence_aa_original'] == item2['sequence_aa_original'] \
                        and item['sequence_aa_alternative'] == item2['sequence_aa_alternative'] \
                        and item['product'] == item2['product']:
                    numerator = item2['total']

            if denominator == 0:
                fraction = 0
            else:
                fraction = (numerator / denominator)
            if denominator_target == 0:
                fraction_target = 0
            else:
                fraction_target = (item['total'] / denominator_target)

            single_line = {'lineage': lineage, 'target': target, 'background': background,
                           'count_seq': item['total'],
                           'product': item['product'],
                           'start_aa_original': item['start_aa_original'],
                           'sequence_aa_original': item['sequence_aa_original'],
                           'sequence_aa_alternative': item['sequence_aa_alternative'],
                           'numerator': numerator,
                           'denominator': denominator,
                           'fraction': fraction*100,
                           'denominator_target': denominator_target,
                           'fraction_target': fraction_target * 100}

            array_result.append(single_line)

        return array_result


@api.route('/analyzeMutationTargetBackgroundFree')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        array_protein = payload['protein']
        query_target = payload['query_target']
        query_background = payload['query_background']
        remove_overlapping = payload['removeOverlapping']

        target = 'empty'
        background = 'empty'

        if 'lineage' in query_target:
            lineage_target = query_target['lineage']
        else:
            lineage_target = 'empty'
        if 'lineage' in query_background:
            lineage_background = query_background['lineage']
        else:
            lineage_background = 'empty'

        j = 0
        i = 0
        where_part_target = ""
        where_part_background = ""
        if query_target is not None:
            for key in query_target:
                if key == 'minDate':
                    if i == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date >= '{query_target[key]}' """
                elif key == 'maxDate':
                    if i == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date <= '{query_target[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_target[key]:
                            j = 0
                            for geoToExclude in query_target[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part_target += f""" WHERE """
                                else:
                                    where_part_target += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part_target += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if i == 0:
                            where_part_target += f""" WHERE """
                        else:
                            where_part_target += f""" AND """
                        replace_fields_value = query_target[key].replace("'", "''")
                        where_part_target += f""" {key} = '{replace_fields_value}' """
                i = i + 1

        if query_background is not None:
            for key in query_background:
                if key == 'minDate':
                    if j == 0:
                        where_part_background += f""" WHERE """
                    else:
                        where_part_background += f""" AND """
                    where_part_background += f""" collection_date >= '{query_background[key]}' """
                elif key == 'maxDate':
                    if j == 0:
                        where_part_background += f""" WHERE """
                    else:
                        where_part_background += f""" AND """
                    where_part_background += f""" collection_date <= '{query_background[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_background[key]:
                            j = 0
                            for geoToExclude in query_background[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part_background += f""" WHERE """
                                else:
                                    where_part_background += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part_background += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if j == 0:
                            where_part_background += f""" WHERE """
                        else:
                            where_part_background += f""" AND """
                        replace_fields_value = query_background[key].replace("'", "''")
                        where_part_background += f""" {key} = '{replace_fields_value}' """
                j = j + 1

        where_protein = ""
        k = 0
        length = len(array_protein)
        for protein in array_protein:
            protein = protein.replace("'", "''")
            if k == 0:
                where_protein += f""" AND (product = '{protein}' """
            else:
                where_protein += f""" OR product = '{protein}' """
            k = k + 1
            if k == length:
                where_protein += """ ) """

        array_result = []

        overlapping_part_background = " "
        overlapping_part_target = " "
        if remove_overlapping.lower() == 'background' or remove_overlapping.lower() == 'both':
            overlapping_part_background = f""" AND it.sequence_id not in (
                 SELECT distinct it2.sequence_id
                 FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                 {where_part_target}
                 AND hs2.coll_date_precision > 1
            ) """

        if remove_overlapping.lower() == 'target' or remove_overlapping.lower() == 'both':
            overlapping_part_target = f""" AND it.sequence_id not in (
                 SELECT distinct it2.sequence_id
                 FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                 {where_part_background}
                 AND hs2.coll_date_precision > 1
            ) """

        query1 = f""" SELECT distinct ann.product, start_aa_original, sequence_aa_original,
                        sequence_aa_alternative, count(*) as total, array_agg(distinct lineage) as lineage
                        FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                        JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                        JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                        {where_part_target}
                        AND coll_date_precision > 1
                        {overlapping_part_target}
                        {where_protein}
                        GROUP BY ann.product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                        ORDER BY product, start_aa_original """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        query_count_denominator = f""" SELECT count(*)
                        FROM 
                        (
                            SELECT distinct it.sequence_id
                            FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                            {where_part_background}
                            AND hs.coll_date_precision > 1
                            {overlapping_part_background}
                        ) as a """

        res_query_count_denominator = db.engine.execute(query_count_denominator).fetchall()
        flask.current_app.logger.debug(query_count_denominator)
        res_query_count_denominator = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator]

        denominator = res_query_count_denominator[0]['count']

        query_count_denominator_target = f""" SELECT count(*)
                                FROM 
                                (
                                    SELECT distinct it.sequence_id
                                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                    {where_part_target}
                                    AND coll_date_precision > 1
                                    {overlapping_part_target}
                                ) as a """

        res_query_count_denominator_target = db.engine.execute(query_count_denominator_target).fetchall()
        flask.current_app.logger.debug(query_count_denominator_target)
        res_query_count_denominator_target = [{column: value for column, value in row.items()}
                                       for row in res_query_count_denominator_target]

        denominator_target = res_query_count_denominator_target[0]['count']

        query_background = f""" SELECT product, start_aa_original, sequence_aa_original, 
                                sequence_aa_alternative, count(*) as total
                                FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                                JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                                JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                                {where_part_background}
                                AND hs.coll_date_precision > 1
                                {overlapping_part_background}
                                {where_protein}
                                GROUP BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative
                                ORDER BY product, start_aa_original, sequence_aa_original, sequence_aa_alternative"""

        res_query_background = db.engine.execute(query_background).fetchall()
        flask.current_app.logger.debug(query_background)
        res_query_background = [{column: value for column, value in row.items()}
                                for row in res_query_background]

        for item in res_query1:
            numerator = 0
            for item2 in res_query_background:
                if item['start_aa_original'] == item2['start_aa_original'] \
                        and item['sequence_aa_original'] == item2['sequence_aa_original'] \
                        and item['sequence_aa_alternative'] == item2['sequence_aa_alternative'] \
                        and item['product'] == item2['product']:
                    numerator = item2['total']

            if denominator == 0:
                fraction = 0
            else:
                fraction = (numerator / denominator)
            if denominator_target == 0:
                fraction_target = 0
            else:
                fraction_target = (item['total'] / denominator_target)

            single_line = {'lineage': 'empty', 'lineage_target': lineage_target,
                           'lineage_background': lineage_background,
                           'target': target, 'background': background,
                           'count_seq': item['total'],
                           'product': item['product'],
                           'start_aa_original': item['start_aa_original'],
                           'sequence_aa_original': item['sequence_aa_original'],
                           'sequence_aa_alternative': item['sequence_aa_alternative'],
                           'numerator': numerator,
                           'denominator': denominator,
                           'fraction': fraction*100,
                           'denominator_target': denominator_target,
                           'fraction_target': fraction_target * 100}

            array_result.append(single_line)

        return array_result


@api.route('/countOverlappingSequenceTargetBackground')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        query_target = payload['query_target']
        query_background = payload['query_background']

        j = 0
        i = 0
        where_part_target = ""
        where_part_background = ""
        if query_target is not None:
            for key in query_target:
                if key == 'minDate':
                    if i == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date >= '{query_target[key]}' """
                elif key == 'maxDate':
                    if i == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date <= '{query_target[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_target[key]:
                            j = 0
                            for geoToExclude in query_target[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part_target += f""" WHERE """
                                else:
                                    where_part_target += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part_target += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if i == 0:
                            where_part_target += f""" WHERE """
                        else:
                            where_part_target += f""" AND """
                        replace_fields_value = query_target[key].replace("'", "''")
                        where_part_target += f""" {key} = '{replace_fields_value}' """
                i = i + 1

        if query_background is not None:
            for key in query_background:
                if key == 'minDate':
                    if j == 0:
                        where_part_background += f""" WHERE """
                    else:
                        where_part_background += f""" AND """
                    where_part_background += f""" collection_date >= '{query_background[key]}' """
                elif key == 'maxDate':
                    if j == 0:
                        where_part_background += f""" WHERE """
                    else:
                        where_part_background += f""" AND """
                    where_part_background += f""" collection_date <= '{query_background[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_background[key]:
                            j = 0
                            for geoToExclude in query_background[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part_background += f""" WHERE """
                                else:
                                    where_part_background += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part_background += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if j == 0:
                            where_part_background += f""" WHERE """
                        else:
                            where_part_background += f""" AND """
                        replace_fields_value = query_background[key].replace("'", "''")
                        where_part_background += f""" {key} = '{replace_fields_value}' """
                j = j + 1

        query1 = f""" SELECT count(distinct it.sequence_id)
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                    {where_part_background}
                    AND hs.coll_date_precision > 1
                    AND it.sequence_id in (
                         SELECT distinct it2.sequence_id
                         FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                         {where_part_target}
                         AND hs2.coll_date_precision > 1
                    ) """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        return res_query1


@api.route('/selectorQuery')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        # payload = {'field': 'lineage', 'query': {'lineage': 'B.1.1.7', 'country': 'Italy', 'minDate': '2021-03-31', 'maxDate': '2021-05-25'}}
        field_name = payload['field']
        query_fields = payload['query']

        if field_name in query_fields:
            del query_fields[field_name]

        i = 0
        where_part = " "
        if query_fields is not None:
            for key in query_fields:
                if key == 'minDate':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDate':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            j = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part += f""" WHERE """
                                else:
                                    where_part += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if i == 0:
                            where_part += f""" WHERE """
                        else:
                            where_part += f""" AND """
                        field_value = query_fields[key].replace("'", "''")
                        where_part += f""" {key} = '{field_value}' """
                i = i + 1

        query1 = f""" SELECT {field_name} as value, count(distinct it.sequence_id) as count
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                    {where_part}
                    AND {field_name} is NOT null
                    AND coll_date_precision > 1
                    GROUP BY {field_name}
                    ORDER BY count desc """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        return res_query1


@api.route('/getAccessionIds')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def post(self):

        payload = api.payload
        # payload = {'query': {'lineage': 'B.1.1.7', 'country': 'Italy', 'geo_group': 'Europe',
        #                      'minDateTerget': '2021-03-31', 'maxDateTarget': '2021-06-28',
        #                      'start_aa_original': 614, 'sequence_aa_original': 'D',
        #                      'sequence_aa_alternative': 'G', 'product': 'Spike (surface glycoprotein)'},
        #            'query_false': ''}
        query_false_field = payload['query_false']
        query_fields = payload['query']
        query_target = payload['query_target']

        j = 0
        where_part_target = ""
        if query_target != 'empty':
            where_part_target += """ AND it.sequence_id not in (
                                 SELECT distinct it2.sequence_id
                                 FROM sequence as it2 JOIN host_sample as hs2 ON it2.host_sample_id = hs2.host_sample_id
                                """
            for key in query_target:
                if key == 'minDate':
                    if j == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date >= '{query_target[key]}' """
                elif key == 'maxDate':
                    if j == 0:
                        where_part_target += f""" WHERE """
                    else:
                        where_part_target += f""" AND """
                    where_part_target += f""" collection_date <= '{query_target[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            k = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if k == 0 and j == 0:
                                    where_part_target += f""" WHERE """
                                else:
                                    where_part_target += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part_target += f""" {fieldToExclude} != '{geo_value}' """
                                k = k + 1
                    else:
                        if j == 0:
                            where_part_target += f""" WHERE """
                        else:
                            where_part_target += f""" AND """
                        replace_fields_value = query_target[key]
                        if key != 'start_aa_original':
                            replace_fields_value = query_target[key].replace("'", "''")
                        if replace_fields_value != 'empty':
                            where_part_target += f""" {key} = '{replace_fields_value}' """
                j = j + 1
            where_part_target += " ) "

        i = 0
        where_part = " "
        if query_fields is not None:
            for key in query_fields:
                if key == 'minDateBackground':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDateBackground':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                elif key == 'minDateTarget':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date >= '{query_fields[key]}' """
                elif key == 'maxDateTarget':
                    if i == 0:
                        where_part += f""" WHERE """
                    else:
                        where_part += f""" AND """
                    where_part += f""" collection_date <= '{query_fields[key]}' """
                else:
                    if key == 'toExclude':
                        for fieldToExclude in query_fields[key]:
                            j = 0
                            for geoToExclude in query_fields[key][fieldToExclude]:
                                if i == 0 and j == 0:
                                    where_part += f""" WHERE """
                                else:
                                    where_part += f""" AND """
                                geo_value = geoToExclude.replace("'", "''")
                                where_part += f""" {fieldToExclude} != '{geo_value}' """
                                j = j + 1
                    else:
                        if i == 0:
                            where_part += f""" WHERE """
                        else:
                            where_part += f""" AND """
                        field_value = query_fields[key]
                        if key != 'start_aa_original':
                            field_value = query_fields[key].replace("'", "''")
                        if key == query_false_field:
                            if field_value != 'empty':
                                where_part += f""" ( {key} != '{field_value}' OR 
                                                 {key} is null ) """
                        else:
                            if field_value != 'empty':
                                where_part += f""" {key} = '{field_value}' """
                i = i + 1

        query1 = f""" SELECT array_agg(distinct it.accession_id ORDER BY it.accession_id) as acc_ids
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                    JOIN annotation as ann ON ann.sequence_id = it.sequence_id
                    JOIN aminoacid_variant as amin ON amin.annotation_id = ann.annotation_id
                    {where_part} 
                    AND coll_date_precision > 1
                    {where_part_target} """

        res_query1 = db.engine.execute(query1).fetchall()
        flask.current_app.logger.debug(query1)
        res_query1 = [{column: value for column, value in row.items()} for row in res_query1]

        return res_query1


@api.route('/allGeo')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def get(self):

        query = f"""SELECT distinct geo_group, country, region, province, count(distinct sequence_id)
                    FROM sequence as it JOIN host_sample as hs ON it.host_sample_id = hs.host_sample_id
                    WHERE coll_date_precision > 1
                    AND collection_date > '2019-01-01'
                    GROUP BY geo_group, country, region, province
                    ORDER BY geo_group, country, region, province """

        res_all = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res_all = [{column: value for column, value in row.items()} for row in res_all]

        return res_all


@api.route('/allLineages')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def get(self):

        query = f"""SELECT distinct lineage, count(*) as cnt
                    FROM sequence
                    GROUP BY lineage
                    ORDER BY cnt desc"""

        res_all = db.engine.execute(query).fetchall()
        flask.current_app.logger.debug(query)
        res_all = [{column: value for column, value in row.items()} for row in res_all]

        return res_all


@api.route('/allEpitopes')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def get(self):

        empty = []
        if all_epitope_dict == empty:
            print("epi empty")

            query = f"""
                    SELECT DISTINCT 
                        iedb_epitope_id, 
                        protein_name, 
                        epitope_sequence,
                        epi_annotation_start, 
                        epi_annotation_stop, 
                        external_link,
                        is_linear,
                        epi_frag_annotation_start,
                        epi_frag_annotation_stop
                    FROM epitope
                    NATURAL JOIN epitope_fragment
                    WHERE virus_id =1 
                    AND host_id = 1
        
                """

            #connection = db.engine.raw_connection()
            #cur = connection.cursor()
            #cur.execute(query)
            #rows_epitope = cur.fetchall()

            #colnames = [desc[0] for desc in cur.description]
            #all_epitopes = pd.DataFrame(rows_epitope, columns=colnames)
            #epitopes = all_epitopes[all_epitopes.protein_name == "Spike (surface glycoprotein)"]
            #epitopes['number_of_pubs'] = epitopes.external_link.str.count(",") + 1
            #epitopes['pubs'] = epitopes.external_link.str.split(",")

            #epitopes_dict = []
            #for index, row in list(epitopes.iterrows()):
            #    epitopes_dict.append(dict(row))

            res = db.engine.execute(query).fetchall()
            res = [{column: value for column, value in row.items()} for row in res]
            res2 = []
            for row in res:
                filter_protein = False
                new_row = row.copy()
                for item in row:
                    if item == "external_link":
                        num = row[item].count(",") + 1
                        new_row[item] = num
                        new_row['pubs'] = row[item].split(",")
                    if item == "protein_name":
                        if row[item] == "Spike (surface glycoprotein)":
                            filter_protein = True
                if filter_protein:
                    res2.append(new_row)
            epitopes_dict = res2

            return epitopes_dict
        else:
            print("epi NOT empty")
            return all_epitope_dict


@api.route('/allImportantMutations')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    def get(self):

        query_mutations = f""" WITH 
                lineage_tot AS(
                select s.lineage, count(*) as lin_tot
                from sequence s
                natural join host_sample
                group by s.lineage
                ),
                array_changes AS(
                select s.lineage, lineage_tot.lin_tot, concat(split_part(product, ' ', 1),'_', 
                sequence_aa_original, start_aa_original, sequence_aa_alternative) as changes
                from sequence s
                natural join host_sample
                natural join annotation
                natural join aminoacid_variant
                join lineage_tot on lineage_tot.lineage = s.lineage
                group by s.lineage, lineage_tot.lin_tot, concat(split_part(product, ' ', 1),'_', sequence_aa_original,
                 start_aa_original, sequence_aa_alternative)
                having ROUND(CAST((count(*)/lineage_tot.lin_tot::float)*100 AS NUMERIC), 2) >= 75
                )
                select lineage, ac.lin_tot as lin_sequences, array_agg(ac.changes order by ac.changes) as common_changes
                from array_changes ac
                group by lineage, ac.lin_tot; """

        res = db.engine.execute(query_mutations).fetchall()
        res = [{column: value for column, value in row.items()} for row in res]

        return res


######################################## END API NEW PROJECT #####################################################


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
        if column == f"{epitope_id}" and not isinstance(values, list):
            where_part += add_and(i, field_name)
            where_part += f" epic.{epitope_id} = {values} "
        elif column == "startExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epic.epi_frag_annotation_stop >= {value} "
        elif column == "stopExt":
            if field_name != "position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epic.epi_frag_annotation_start <= {value} "
        elif column == "startExtVariant":
            if field_name != "variant_position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epic.start_aa_original >= {value} "
        elif column == "stopExtVariant":
            if field_name != "variant_position_range":
                for value in values:
                    where_part += add_and( i, field_name)
                    where_part += f" epic.start_aa_original <= {value} "
        elif column == "startFreqExt" or column == "stopFreqExt":
            if column == "startFreqExt":
                if field_name != "response_frequency":
                    for value in values:
                        if value is not None:
                            where_part += add_and( i, field_name)
                            where_part += f" epic.response_frequency_pos >= {value} "
                        else:
                            where_part += add_and( i, field_name)
                            where_part += f" epic.response_frequency_pos IS NULL "
            elif column == "stopFreqExt":
                if field_name != "response_frequency":
                    for value in values:
                        if value is not None:
                            where_part += add_and( i, field_name)
                            where_part += f" epic.response_frequency_pos <= {value} "
                        else:
                            where_part += add_and( i, field_name)
                            where_part += f" epic.response_frequency_pos IS NULL "
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
                            where_part += f"( epic.mhc_allele LIKE '%{value},%' or epic.mhc_allele LIKE '%{value}')"
                        else:
                            where_part += f" epic.{column} ="
                            if column_type == 'str':
                                where_part += f" '{value}' "
                            elif column_type == 'num':
                                where_part += f" {value} "
                            else:
                                where_part += ""
                    else:
                        where_part += f" epic.{column} IS NULL "
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
        #and not isinstance(values, list)
        if column == f"{epitope_id}" and not isinstance(values, list):
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
                                         WHEN epic.sequence_id = seqc.sequence_id THEN 1
                                         ELSE 0
                                    END) / count(distinct epic.cell_type)  as {header}"""
                                    # WHEN epic.sequence_id = seqc.sequence_id THEN variant_aa_length
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
            if header == "external_link" or header == f'mhc_allele' or header == f'response_frequency_pos' \
                    or header == f'mhc_class' or header == f'assay_type':
                group_by_part += f""
            else:
                if header == "cell_type":
                    group_by_part += f" a.all_array_info "
                else:
                    group_by_part += f"a.{header}"

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop' or header == "external_link" or header == f'mhc_allele' \
                    or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
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
        elif header == f'external_link':
            table_select_part += f" array_agg(distinct {header}) as {header} "
        elif header == f'cell_type':
            table_select_part += f" a.all_array_info "
        elif header == f'mhc_allele' or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
            table_select_part += ""
        else:
            table_select_part += f" a.{header} "

        count = count - 1
        if count > 0:
            if header == 'epi_frag_annotation_start' or header == 'epi_frag_annotation_stop' or header == f'mhc_allele' \
                    or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
                table_select_part += ""
            else:
                table_select_part += ', '

    table_select_part += f" FROM ( SELECT "

    count = len(payload_table_headers)
    for header in payload_table_headers:
        if header == f'{epitope_id}':
            table_select_part += f"epic.{epitope_id} "
        elif header == 'num_seq':
            table_select_part += f"count(distinct(seqc.sequence_id)) as {header}"
        elif header == 'num_var':
            table_select_part += f"""sum(CASE
                                         WHEN epic.sequence_id = seqc.sequence_id THEN 1
                                         ELSE 0
                                    END) / count(distinct epic.cell_type)  as {header}"""
                                    # WHEN epic.sequence_id = seqc.sequence_id THEN variant_aa_length
        elif header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                or header == 'epi_frag_annotation_stop':
            table_select_part += ""
        else:
            if header == "is_linear":
                table_select_part += f" bool_and(epic.{header}) as {header} "
            elif header == f'cell_type':
                table_select_part += f"array_agg(distinct array[epic.cell_type, epic.mhc_allele," \
                                     f" epic.response_frequency_pos::text, epic.mhc_class, epic.assay_type]) " \
                                     f"as all_array_info"
            else:
                if header == "external_link" or header == f'mhc_allele' or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
                    table_select_part += f""
                else:
                    table_select_part += f" max(epic.{header}) as {header} "

        count = count - 1
        if count > 0:
            if header == 'epi_fragment_sequence' or header == 'epi_frag_annotation_start' \
                    or header == 'epi_frag_annotation_stop' or header == "external_link" or header == f'mhc_allele' \
                    or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
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
        elif header == f'external_link':
            table_select_part += f" array_agg(distinct {header}) as {header} "
        elif header == f'cell_type':
            table_select_part += f"array_agg(distinct array[cell_type, mhc_allele," \
                                 f" response_frequency_pos::text, mhc_class, assay_type]) as all_array_info"
        elif header == f'mhc_allele' or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
            table_select_part += f""
        else:
            table_select_part += f" max({header}) as {header} "

        count = count - 1
        if count > 0:
            if header == f'mhc_allele' or header == f'response_frequency_pos' or header == f'mhc_class' or header == f'assay_type':
                table_select_part += ''
            else:
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
            table_select_part += f"count(variant_aa_length ) as {header}"
                            # "sum(variant_aa_length ) as {header}"
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


lineage_stats_dict = {}


def create_lineage_stats_dict():
    query_lin_count = f"""
            SELECT lineage, count(*) as total
            FROM sequence
            WHERE virus_id = 1
            GROUP BY lineage
            """

    res_lin_count = db.engine.execute(query_lin_count).fetchall()
    flask.current_app.logger.debug(query_lin_count)

    lin_dict = {}
    for row in res_lin_count:
        row_dict = dict(row)
        if row_dict['lineage'] is not None:
            lineage = row_dict['lineage']
        else:
            lineage = None
        lin_dict[lineage] = row_dict

    # res_lin_count = [{column: value for column, value in row.items()} for row in res_lin_count]

    query_all = f"""
            SELECT lineage, product, sequence_aa_original, start_aa_original, sequence_aa_alternative, count(*) as total
            FROM sequence
            NATURAL JOIN annotation
            NATURAL JOIN aminoacid_variant
            WHERE virus_id = 1
            GROUP BY lineage, product, sequence_aa_original, start_aa_original, sequence_aa_alternative
            ORDER BY start_aa_original asc
            """

    res_all = db.engine.execute(query_all).fetchall()
    flask.current_app.logger.debug(query_all)

    res_all = [{column: value for column, value in row.items()} for row in res_all]

    for row in res_all:
        for item in row:
            new_row = {}
            if row['lineage'] in lineage_stats_dict:
                if item == "total":
                    line = lineage_stats_dict[row['lineage']]
                    name = (row['start_aa_original'], row['sequence_aa_original'], row['sequence_aa_alternative'],
                            row['product'])
                    denominator = lin_dict[row['lineage']]
                    line[name] = row[item] / denominator['total']
            else:
                if item == "total":
                    name = (row['start_aa_original'], row['sequence_aa_original'], row['sequence_aa_alternative'],
                            row['product'])
                    denominator = lin_dict[row['lineage']]
                    new_row[name] = row[item] / denominator['total']
                lineage_stats_dict[row['lineage']] = new_row


all_epitope_dict = []


def create_all_epitope_dict():
    query = f"""
                    SELECT DISTINCT 
                        iedb_epitope_id, 
                        protein_name, 
                        epitope_sequence,
                        epi_annotation_start, 
                        epi_annotation_stop, 
                        external_link,
                        is_linear,
                        epi_frag_annotation_start,
                        epi_frag_annotation_stop
                    FROM epitope
                    NATURAL JOIN epitope_fragment
                    WHERE virus_id =1 
                    AND host_id = 1

                """

    res = db.engine.execute(query).fetchall()
    res = [{column: value for column, value in row.items()} for row in res]
    for row in res:
        filter_protein = False
        new_row = row.copy()
        for item in row:
            if item == "external_link":
                num = row[item].count(",") + 1
                new_row[item] = num
                new_row['pubs'] = row[item].split(",")
            if item == "protein_name":
                if row[item] == "Spike (surface glycoprotein)":
                    filter_protein = True
        if filter_protein:
            all_epitope_dict.append(new_row)
