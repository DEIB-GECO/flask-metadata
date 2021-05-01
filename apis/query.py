import datetime
import itertools

import flask
import sqlalchemy
from flask import Response, json
from flask_restplus import Namespace, Resource, fields, inputs, marshal

from model.models import db
from utils import sql_query_generator, log_query
from .epitope import gen_where_epi_query_field, getMatView
from .poll import poll_cache

is_gisaid = True

api = Namespace('query', description='Operations to perform queries using metadata')

query = api.model('Query', {
})

parser = api.parser()
parser.add_argument('body', type="json", help='json ', location='json')
parser.add_argument('rel_distance', type=int, default=3)

count_parser = api.parser()
count_parser.add_argument('body', type="json", help='json ', location='json')
count_parser.add_argument('agg', type=inputs.boolean, default=False)
count_parser.add_argument('rel_distance', type=int, default=3)
count_parser.add_argument('annotation_type', type=str)
count_parser.add_argument('is_control', type=inputs.boolean, default=False)
count_parser.add_argument('gisaid_only', type=inputs.boolean, default=False)

table_parser = api.parser()
table_parser.add_argument('body', type="json", help='json ', location='json')
table_parser.add_argument('agg', type=inputs.boolean, default=False)
table_parser.add_argument('page', type=int)
table_parser.add_argument('num_elems', type=int)
table_parser.add_argument('order_col', type=str, default='accession_id')
table_parser.add_argument('order_dir', type=str, default='asc')
table_parser.add_argument('rel_distance', type=int, default=3)
table_parser.add_argument('annotation_type', type=str)
table_parser.add_argument('is_control', type=inputs.boolean, default=False)
table_parser.add_argument('download_file_format', type=str, default="fasta")
table_parser.add_argument('download_type', type=str, default="nuc")
table_parser.add_argument('gisaid_only', type=inputs.boolean, default=False)

################################API IMPLEMENTATION###########################################


query_result = api.model('QueryResult', {
    # ITEM
    'accession_id': fields.String,
    'strain_name': fields.String,
    'is_reference': fields.String,
    'is_complete': fields.String,
    # 'nucleotide_sequence': fields.String,
    # 'amino_acid_sequence': fields.String,
    'strand': fields.String,
    'length': fields.String,
    'gc_percentage': fields.String,
    'n_percentage': fields.String,
    'lineage': fields.String,
    'clade': fields.String,
    'sequencing_technology': fields.String,
    'assembly_method': fields.String,
    'coverage': fields.String,
    'sequencing_lab': fields.String,
    'submission_date': fields.String,
    'bioproject_id': fields.String,
    'database_source': fields.String,
    'taxon_id': fields.String,
    'taxon_name': fields.String,
    'family': fields.String,
    'sub_family': fields.String,
    'genus': fields.String,
    'species': fields.String,
    'equivalent_list': fields.String,
    'molecule_type': fields.String,
    'is_single_stranded': fields.String,
    'is_positive_stranded': fields.String,
    'host_taxon_id': fields.String,
    'host_taxon_name': fields.String,
    'gender': fields.String,
    'age': fields.String,
    'collection_date': fields.String,
    'isolation_source': fields.String,
    'originating_lab': fields.String,
    'country': fields.String,
    'region': fields.String,
    'geo_group': fields.String,
    '*_count': fields.Wildcard(fields.String),
})

################################API DOCUMENTATION STRINGS###################################
body_desc = 'It must be in the format {\"gcm\":{},\"type\":\"original\",\"kv\":{}}.\n ' \
            'Example values for the three parameters: \n ' \
            '- gcm may contain \"disease\":[\"prostate adenocarcinoma\",\"prostate cancer\"],\"assembly\":[\"grch38\"]\n ' \
            '- type may be original, synonym or expanded\n ' \
            '- kv may contain \"tumor_0\":{\"type_query\":\"key\",\"exact\":false,\"query\":{\"gcm\":{},\"pairs\":{\"biospecimen__bio__tumor_descriptor\":[\"metastatic\"]}}}'

agg_desc = 'Agg is true for aggregated view (one row per each item, potentially multiple values for an attribute are separated with \\|).\n' \
           'Agg is false for replicated view (one row for each Replicate/Biosample/Donor generating the item).'

page_desc = 'Progressive number of page of results to retrieve.'

num_elems_desc = 'Number of resulting items to retrieve per page.'

order_col_desc = 'Name of column on which table order is based.'

order_dir_desc = 'Order of column specified in order_col parameter: asc (ascendant) or desc (descendant).'

rel_distance_desc = 'When type is \'expanded\', it indicates the depth of hyponyms in the ontological hierarchy to consider.'

deprecated_desc = "## In the next release, the endpoint will not be available\n" + \
                  "## Please use */field/{field_name}* endpoint\n" + \
                  "------------------\n"


#############################Query Function#############################################
def full_query(filter_in, q_type, pairs, agg, orderCol, orderDir, rel_distance, annotation_type,
               limit, offset, is_control, gisaid_only, numElems=None, epitope_part=None, epitope_table=None):
    def run_query(limit_inner, offset_inner, exclude_accession_list=None, is_aa=None):
        if exclude_accession_list:
            exclude_accession_list = list(f"{x}" for x in exclude_accession_list)
            if exclude_accession_list:
                exclude_accession_where = f" it.sequence_id NOT IN ({','.join(exclude_accession_list)}) "
            else:
                exclude_accession_where = None

            if is_aa and not is_gisaid:
                exclude_aa_seq_null = f" it.sequence_id  in (SELECT sequence_id FROM annotation WHERE aminoacid_sequence is not null) "
            else:
                exclude_aa_seq_null = None
        else:
            exclude_accession_where = None
            exclude_aa_seq_null = None

        if gisaid_only:
            exclude_gisaid_where = " gisaid_only "
        else:
            exclude_gisaid_where = None

        query = sql_query_generator(filter_in, q_type, pairs, 'table', agg, limit=limit_inner, offset=offset_inner,
                                    order_col=orderCol, order_dir=orderDir, rel_distance=rel_distance,
                                    annotation_type=annotation_type,
                                    external_where_conditions=[exclude_accession_where, exclude_gisaid_where, exclude_aa_seq_null],
                                    epitope_part=epitope_part, epitope_table=epitope_table)

        pre_query = db.engine.execute(sqlalchemy.text(query))
        # return_columns = set(pre_query._metadata.keys)
        res = pre_query  # .fetchall()
        result = (dict(x) for x in res)
        result = ({k: str(v) if type(v) == datetime.date else v for k, v in x.items()} for x in result)
        # for row in res:
        #     row_dict = {str(x): row[x] for x in return_columns}
        #     if annotation_type:
        #         row_dict['nucleotide_sequence'] = row_dict['annotation_view_nucleotide_sequence']
        #         row_dict['amino_acid_sequence'] = row_dict['annotation_view_aminoacid_sequence']
        #     result.append(row_dict)
        flask.current_app.logger.debug("QUERY: ")
        flask.current_app.logger.debug(query)

        flask.current_app.logger.debug('got results')
        return result

    if is_control and pairs:
        result_inner = run_query(limit_inner=None, offset_inner=None)
        # print("len(result_inner)", len(result_inner))
        result_inner_accession = (x['sequence_id'] for x in result_inner)
        is_aa = ('aa' in [pairs[p]['type_query'] for p in pairs])
        pairs = {}
        result_inner2 = run_query(limit, offset, exclude_accession_list=result_inner_accession, is_aa=is_aa)
        # print("len(result_inner2)", len(result_inner2))
        return_result = result_inner2
    else:
        result_inner = run_query(limit_inner=limit, offset_inner=offset)
        # print("len(result_inner)", len(result_inner))
        return_result = result_inner

    # return_result = [remove_date_in_dict(x) for x in return_result]
    # query_result_dict = dict(query_result)
    # print(type(query_result))
    # return_result = jsonify(marshal(return_result, query_result_dict))
    return return_result


#############################SERVICES IMPLEMENTATION#############################################
@api.route('/table')
@api.response(404, 'Results not found')  # TODO correct
class Query(Resource):
    @api.doc('return_query_result', params={'body': body_desc,
                                            'agg': agg_desc,
                                            'page': page_desc,
                                            'num_elems': num_elems_desc,
                                            'order_col': order_col_desc,
                                            'order_dir': order_dir_desc,
                                            'rel_distance': rel_distance_desc})
    # @api.marshal_with(query_result)
    @api.expect(table_parser)
    def post(self):
        '''For the posted query, it retrieves a list of items with the related GCM metadata'''

        payload = api.payload
        args = table_parser.parse_args()
        rel_distance = args['rel_distance']
        agg = args['agg']
        orderCol = args['order_col']
        orderDir = args['order_dir']
        if orderCol == "null":
            orderCol = "accession_id"

        numPage = args['page']
        numElems = args['num_elems']

        annotation_type = args.get('annotation_type')

        is_control = args.get('is_control')
        gisaid_only = args.get('gisaid_only')

        if numPage and numElems:
            offset = (numPage - 1) * numElems
            limit = numElems
        else:
            offset = None
            limit = None

        filter_in = payload.get("gcm")

        q_type = payload.get("type")
        pairs = payload.get("kv")

        epitope_part = payload.get("epitope")
        if epitope_part is not None:
            epitope_table = getMatView(filter_in['taxon_name'], epitope_part['product'])
            field_name = "toTable"
            #epitope_part = f" WHERE iedb_epitope_id = {epitope_part['iedb_epitope_id']}"
            epitope_part = gen_where_epi_query_field(epitope_part, field_name)
        else:
            epitope_table = None

        return_result = list(full_query(filter_in, q_type, pairs, agg, orderCol, orderDir, rel_distance, annotation_type,
                                   limit, offset, is_control, gisaid_only, numElems, epitope_part=epitope_part, epitope_table=epitope_table))
        return return_result


count_result = api.model('QueryResult', {
    'name': fields.String,
    'count': fields.Integer,
})


@api.route('/count')
@api.response(404, 'Results not found')  # TODO correct
class QueryCountDataset(Resource):
    @api.doc('return_query_result1', params={'body': body_desc,
                                             'agg': agg_desc,
                                             'annotation_type': 'No description',
                                             'rel_distance': rel_distance_desc})
    @api.expect(count_parser)
    def post(self):
        '''For the posted query, it retrieves the total number of item rows'''
        payload = api.payload
        log_query('query/count', '', payload)

        filter_in = payload.get('gcm')
        q_type = payload.get('type')
        pairs = payload.get('kv')
        args = count_parser.parse_args()
        agg = args['agg']
        rel_distance = args['rel_distance']
        annotation_type = args.get('annotation_type')

        is_control = args.get('is_control')
        gisaid_only = args.get('gisaid_only')

        epitope_part = payload.get("epitope")

        if epitope_part is not None:
            epitope_table = getMatView(filter_in['taxon_name'], epitope_part['product'])
            field_name = "toTable"
            epitope_part = gen_where_epi_query_field(epitope_part, field_name)
        else:
            epitope_table = None

        def run_query(is_aa=False):
            if is_aa and not is_gisaid:
                exclude_aa_seq_null = f" it.sequence_id  in (SELECT sequence_id FROM annotation WHERE aminoacid_sequence is not null) "
            else:
                exclude_aa_seq_null = None
            if gisaid_only:
                exclude_gisaid_where = " gisaid_only "
            else:
                exclude_gisaid_where = None

            query = "select count(*) "
            query += "from ("

            sub_query = sql_query_generator(filter_in, q_type, pairs, 'table', agg=agg, limit=None, offset=None,
                                            rel_distance=rel_distance, annotation_type=annotation_type,
                                            external_where_conditions=[exclude_gisaid_where,exclude_aa_seq_null], epitope_part=epitope_part, epitope_table=epitope_table)
            query += sub_query + ") as a "
            flask.current_app.logger.debug(query)

            res = db.engine.execute(sqlalchemy.text(query)).fetchall()
            flask.current_app.logger.debug('got results')
            return res[0][0]

        if is_control and pairs:
            result_inner = run_query()
            is_aa = ('aa' in [pairs[p]['type_query'] for p in pairs])
            # add is_aa to the count
            pairs = {}
            result_inner2 = run_query(is_aa=is_aa)
            return_result = result_inner2 - result_inner
        else:
            result_inner = run_query()
            return_result = result_inner

        return return_result


@api.route('/countPoll')
@api.response(404, 'Results not found')
class QueryCountDataset(Resource):
    @api.doc('return_query_result1', params={'body': body_desc,
                                             'agg': agg_desc,
                                             'annotation_type': 'No description',
                                             'rel_distance': rel_distance_desc})
    @api.expect(count_parser)
    def post(self):
        '''For the posted query, it retrieves the total number of item rows'''
        payload = api.payload
        log_query('query/count', '', payload)

        filter_in = payload.get('gcm')
        q_type = payload.get('type')
        pairs = payload.get('kv')
        args = count_parser.parse_args()
        agg = args['agg']
        rel_distance = args['rel_distance']
        annotation_type = args.get('annotation_type')

        is_control = args.get('is_control')

        epitope_part = payload.get("epitope")

        if epitope_part is not None:
            epitope_table = getMatView(filter_in['taxon_name'], epitope_part['product'])
            field_name = "toTable"
            epitope_part = gen_where_epi_query_field(epitope_part, field_name)
        else:
            epitope_table = None

        def run_query(is_aa=False):
            poll_id = poll_cache.create_dict_element()
            def async_function():
                try:
                    if is_aa and not is_gisaid:
                        exclude_aa_seq_null = f" it.sequence_id  in (SELECT sequence_id FROM annotation_sequence) "
                    else:
                        exclude_aa_seq_null = None
                    query = "select count(*) "
                    query += "from ("

                    sub_query = sql_query_generator(filter_in, q_type, pairs, 'table', agg=agg, limit=None, offset=None,
                                                    rel_distance=rel_distance, annotation_type=annotation_type,
                                                    external_where_conditions=[exclude_aa_seq_null], epitope_part=epitope_part,
                                                    epitope_table=epitope_table)

                    query += sub_query + ") as a "
                    flask.current_app.logger.debug(query)

                    res = db.engine.execute(sqlalchemy.text(query)).fetchall()
                    flask.current_app.logger.debug('got results')

                    poll_cache.set_result(poll_id, res[0][0])
                except Exception as e:
                    poll_cache.set_result(poll_id, None)
                    raise e

            from app import executor_inner
            executor_inner.submit(async_function)
            return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')

        if is_control and pairs:
            result_inner = run_query()
            is_aa = ('aa' in [pairs[p]['type_query'] for p in pairs])
            # add is_aa to the count
            pairs = {}
            result_inner2 = run_query(is_aa=is_aa)
            return_result = result_inner2 - result_inner
        else:
            result_inner = run_query()
            return_result = result_inner

        return return_result


# TODO check code repetition
@api.route('/download')
@api.hide
@api.response(404, 'Results not found')  # TODO correct
class QueryDownload(Resource):
    @api.doc('return_query_result4', params={'body': body_desc,
                                             'rel_distance': rel_distance_desc})
    @api.expect(parser)
    def post(self):
        '''For the items selected by the posted query, it retrieves URIs for download from our system'''

        '''For the posted query, it retrieves a list of items with the related GCM metadata'''

        payload = api.payload
        args = table_parser.parse_args()
        rel_distance = args['rel_distance']
        agg = args['agg']
        orderCol = args['order_col']
        orderDir = args['order_dir']
        if orderCol == "null":
            orderCol = "accession_id"

        numPage = args['page']
        numElems = args['num_elems']

        annotation_type = args.get('annotation_type')

        is_control = args.get('is_control')

        if numPage and numElems:
            offset = (numPage - 1) * numElems
            limit = numElems
        else:
            offset = None
            limit = None

        filter_in = payload.get("gcm")

        q_type = payload.get("type")
        pairs = payload.get("kv")

        epitope_part = payload.get("epitope")
        if epitope_part is not None:
            epitope_table = getMatView(filter_in['taxon_name'], epitope_part['product'])
            field_name = "toTable"
            epitope_part = gen_where_epi_query_field(epitope_part, field_name)
        else:
            epitope_table = None

        return_result = full_query(filter_in, q_type, pairs, agg, orderCol, orderDir, rel_distance, annotation_type,
                                   limit, offset, is_control, with_nuc_seq=True, epitope_part=epitope_part, epitope_table=epitope_table)

        downloadFileFormat = args['download_file_format']
        downloadType = args['download_type']

        if downloadType == "aa":
            res = ((x["accession_id"], x["aminoacid_sequence"]) for x in return_result)
        else:
            if annotation_type:
                res = ((x["accession_id"], x["annotation_nucleotide_sequence"]) for x in return_result)
            else:
                res = ((x["accession_id"], x["nucleotide_sequence"]) for x in return_result)

        res = (x for x in res if x[1])

        if downloadFileFormat == "fasta":
            n = 60
            res = (itertools.chain((f">{acc}",), (seq[i:i + n] for i in range(0, len(seq), n))) for acc, seq in res)
            res = itertools.chain.from_iterable(res)
        else:
            res = itertools.chain([["accession_id", "sequence"]], res)
            res = (",".join(x) for x in res)

        res = (x + "\n" for x in res)

        # res = "\n".join(",".join(x) for x in res)

        # return return_result

        return Response(res, mimetype='text/plain')
