from collections import defaultdict
from datetime import date
from itertools import groupby, chain

import flask
import sqlalchemy
from flask import json
from flask_restplus import Namespace, Resource, inputs

from apis.poll import poll_cache
from apis.query import full_query
from model.models import db
from utils import taxon_name_dict, taxon_id_dict

is_gisaid = False

api = Namespace('viz', description='Operations to perform viz using metadata')

table_parser = api.parser()
table_parser.add_argument('body', type="json", help='json ', location='json')
table_parser.add_argument('is_control', type=inputs.boolean, default=False)

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

with open("viz_schema.json",'r') as f:
    schema = json.load(f)
schema_names = {x["name"] for x in schema}


#############################SERVICES IMPLEMENTATION#############################################
@api.route('/submit')
@api.response(404, 'Results not found')  # TODO correct
class VizSubmit(Resource):
    @api.doc('return_query_result', params={'body': body_desc,
                                            'is_control': "is_control desc"})
    @api.expect(table_parser)
    def post(self):
        '''For the posted query, it retrieves a list of items with the related GCM metadata'''

        payload = api.payload
        args = table_parser.parse_args()
        is_control = args.get('is_control')

        filter_in = payload.get("gcm")
        q_type = payload.get("type")
        pairs = payload.get("kv")

        # region Find virus information
        if 'taxon_id' in filter_in and len(filter_in["taxon_id"]) == 1:
            the_virus = taxon_id_dict[filter_in['taxon_id'][0]]
        elif 'taxon_name' in filter_in and len(filter_in["taxon_name"]) == 1:
            the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
        else:
            the_virus = None
            flask.current_app.logger.debug(f"SINGLE VIRUS PROBLEM")
            api.abort(422, "Please select only one virus by using virus taxonomy name or taxonomy ID.")
        reference_sequence_length = the_virus["nucleotide_sequence_length"]
        taxon_id = the_virus["taxon_id"]
        taxon_name = the_virus["taxon_name"]
        n_products = the_virus["n_products"]
        a_products = the_virus["a_products"]

        # endregion

        poll_id = poll_cache.create_dict_element()

        def async_function():
            try:
                res = full_query(filter_in, q_type, pairs, orderCol="sequence_id", limit=None, is_control=is_control,
                                 agg=False, orderDir="ASC", rel_distance=3, annotation_type=None, offset=0)

                res_sequence_id = [str(row["sequence_id"]) for row in res]

                # region Nucleotide variant part
                query = f"""
                    SELECT  sequence_id,
                            start_original,   
                            sequence_original,   
                            sequence_alternative,   
                            variant_type,
                            variant_length,
                            array_agg(DISTINCT ARRAY[effect, putative_impact, impact_gene_name])
                    FROM nucleotide_variant
                    NATURAL JOIN variant_impact
                    WHERE sequence_id IN ({','.join(res_sequence_id)}) 
                    GROUP BY sequence_id, start_original, sequence_original, sequence_alternative, variant_type, variant_length
                    ORDER BY sequence_id, start_original, sequence_original, sequence_alternative, variant_type, variant_length
                """

                print(query)
                pre_query = db.engine.execute(sqlalchemy.text(query))
                res_nuc = pre_query  # .fetchall()
                res_nuc = groupby(res_nuc, lambda x: x[0])
                res_nuc = defaultdict(list, (
                    (sequence_id,
                     list(chain(*(
                         zip(range(pos, pos + length), orig, alt, [var_type] * length,
                             [impacts] * length) if var_type != 'INS' else
                         [[pos, orig, alt, var_type, impacts]]
                         for _, pos, orig, alt, var_type, length, impacts in rows))))
                    for sequence_id, rows in res_nuc
                ))
                # endregion

                # region Amino acid variant part
                # TODO remove "AND start_aa_original is not null"
                query = f"""
                    SELECT  sequence_id,
                            product,   
                            start_aa_original,   
                            sequence_aa_original,
                            sequence_aa_alternative,
                            variant_aa_type,
                            variant_aa_length
                    FROM annotation 
                    NATURAL JOIN aminoacid_variant
                    WHERE sequence_id IN ({','.join(res_sequence_id)}) 
        AND start_aa_original is not null
                    order by sequence_id, product, start_aa_original  
                """

                print(query)
                pre_query = db.engine.execute(sqlalchemy.text(query))
                res_aa_pre = pre_query  # .fetchall()
                res_aa_pre = groupby(res_aa_pre, lambda x: (x[0], x[1]))

                res_aa = defaultdict(dict)
                for (sequence_id, product), rows in res_aa_pre:
                    res_aa[sequence_id][product] = list(chain(*(
                        zip(range(pos, pos + length), orig, alt, [var_type] * length) if var_type != 'INS'
                        else [[pos, orig, alt, var_type]]
                        for _, _, pos, orig, alt, var_type, length in rows)))

                # print(res_aa)
                # endregion

                sequences = {
                    row['accession_id']:
                        {
                            "id": row['accession_id'],
                            "meta": {k: (v if not isinstance(v, date) else str(v)) for k, v in row.items() if
                                     k in schema_names},
                            "closestSequences": [],
                            "variants": {
                                "N": {
                                    "schema": ["position", "from", "to", "type", ["effect", "putative_impact", "gene"]],
                                    "variants": res_nuc[row["sequence_id"]],
                                },
                                "A": {
                                    "schema": ["position", "from", "to", "type"],
                                    "variants": res_aa[row["sequence_id"]],
                                }
                            },
                            "sequence": row["nucleotide_sequence"]
                        }
                    for row in res
                }

                result = {
                    'sequencesCount': len(res),
                    'taxon_id': taxon_id,
                    'taxon_name': taxon_name,
                    "referenceSequence": {"length": reference_sequence_length},
                    "schema": schema,
                    "products": {
                        "A": a_products,
                        "N": n_products,
                    },
                    "sequences": sequences
                }
                print("PRE poll_cache.set_result(poll_id, result)")
                poll_cache.set_result(poll_id, result)
                print("POST poll_cache.set_result(poll_id, result)")

                # region DEBUG_FILE_WRITE
                # if False:
                #     print("FILE WRITING")
                #     file_name = flask.request.args.get("file_name", "data_test_default")
                #     with open(f'{file_name}.json', 'w') as outfile:
                #         json.dump({"result": result}, outfile, cls=MyJSONEncoder, sort_keys=False)
                #     print("FILE WRITTEN")
                # endregion
            except Exception as e:
                print(e)
                poll_cache.set_result(poll_id, None)

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')

# Request URL: http://geco.deib.polimi.it/virusurf/api/query/table?is_control=false&page=1&num_elems=10&order_col=accession_id&order_dir=ASC
# Request Payload: {"gcm":{"taxon_name":["severe acute respiratory syndrome coronavirus 2"],"country":["italy"]},"type":"original","kv":{}}

# curl -X POST "http://localhost:5000/virusurf/api/viz/table?is_control=false" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"gcm\":{\"taxon_name\":[\"severe acute respiratory syndrome coronavirus 2\"],\"country\":[\"italy\"]},\"type\":\"original\",\"kv\":{}}"
