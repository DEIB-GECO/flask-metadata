import base64
import gzip
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
from .epitope import gen_where_epi_query_field, gen_epitope_part_json_virusviz, gen_epitope_part_json_virusviz2, \
    getMatView

is_gisaid = False

api = Namespace('viz', description='Operations to perform viz using metadata')

table_parser = api.parser()
table_parser.add_argument('body', type="json", help='json ', location='json')
table_parser.add_argument('is_control', type=inputs.boolean, default=False)
table_parser.add_argument('aa_only', type=inputs.boolean, default=False)
table_parser.add_argument('gisaid_only', type=inputs.boolean, default=False)

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


with open("viz_schema.json", 'r') as f:
    schema = json.load(f)
schema_names = {x["name"] for x in schema}

sars_cov_2_products = {
    "A": [
        {
            "name": "E (envelope protein)",
            "start": 26245,
            "end": 26472,
            "row": 0,
            "color": "#7c98b3"
        },
        {
            "name": "M (membrane glycoprotein)",
            "start": 26523,
            "end": 27191,
            "row": 0,
            "color": "#536b78"
        },
        {
            "name": "N (nucleocapsid phosphoprotein)",
            "start": 28274,
            "end": 29533,
            "row": 0,
            "color": "#f68e5f"
        },
        {
            "name": "ORF10 protein",
            "start": 29558,
            "end": 29674,
            "row": 0,
            "color": "#f76c5e"
        },
        {
            "name": "NSP16 (2'-O-ribose methyltransferase)",
            "start": 20659,
            "end": 21552,
            "row": 0,
            "color": "#22577a"
        },
        {
            "name": "NSP3",
            "start": 2720,
            "end": 8554,
            "row": 0,
            "color": "#7209b7"
        },
        {
            "name": "NSP4",
            "start": 8555,
            "end": 10054,
            "row": 0,
            "color": "#560bad"
        },
        {
            "name": "NSP15 (endoRNAse)",
            "start": 19621,
            "end": 20658,
            "row": 0,
            "color": "#38a3a5"
        },
        {
            "name": "NSP5 (3C-like proteinase)",
            "start": 10055,
            "end": 10972,
            "row": 0,
            "color": "#480ca8"
        },
        {
            "name": "NSP14 (3'-to-5' exonuclease)",
            "start": 18040,
            "end": 19620,
            "row": 0,
            "color": "#57cc99"
        },
        {
            "name": "NSP11",
            "start": 13442,
            "end": 13480,
            "row": 0,
            "color": "#65bc6e"
        },
        {
            "name": "NSP13 (helicase)",
            "start": 16237,
            "end": 18039,
            "row": 0,
            "color": "#80ed99"
        },
        {
            "name": "NSP6",
            "start": 10973,
            "end": 11842,
            "row": 0,
            "color": "#3a0ca3"
        },
        {
            "name": "NSP7",
            "start": 11843,
            "end": 12091,
            "row": 0,
            "color": "#3f37c9"
        },
        {
            "name": "NSP8",
            "start": 12092,
            "end": 12685,
            "row": 0,
            "color": "#4361ee"
        },
        {
            "name": "NSP9",
            "start": 12686,
            "end": 13024,
            "row": 0,
            "color": "#4895ef"
        },
        {
            "name": "NSP12 (RNA-dependent RNA polymerase)",
            "start": 13442,
            "end": 16236,
            "row": 0,
            "color": "#c7f9cc"
        },
        {
            "name": "ORF1ab polyprotein",
            "start": 266,
            "end": 21555,
            "row": 0,
            "color": "#89c4be"
        },
        {
            "name": "NSP10",
            "start": 13025,
            "end": 13441,
            "row": 0,
            "color": "#4cc9f0"
        },
        {
            "name": "NSP1 (leader protein)",
            "start": 266,
            "end": 805,
            "row": 0,
            "color": "#f72585"
        },
        {
            "name": "ORF1a polyprotein",
            "start": 266,
            "end": 13483,
            "row": 0
        },
        {
            "name": "NSP2",
            "start": 806,
            "end": 2719,
            "row": 0,
            "color": "#ccb7ae"
        },
        {
            "name": "NS3 (ORF3a protein)",
            "start": 25393,
            "end": 26220,
            "row": 0,
            "color": "#a3a3a3"
        },
        {
            "name": "NS6 (ORF6 protein)",
            "start": 27202,
            "end": 27387,
            "row": 0,
            "color": "#586ba4"
        },
        {
            "name": "NS7a (ORF7a protein)",
            "start": 27394,
            "end": 27759,
            "row": 0,
            "color": "#324376"
        },
        {
            "name": "NS7b (ORF7b)",
            "start": 27756,
            "end": 27887,
            "row": 0,
            "color": "#f5dd90"
        },
        {
            "name": "NS8 (ORF8 protein)",
            "start": 27894,
            "end": 28259,
            "row": 0,
            "color": "#b79738"
        },
        {
            "name": "Spike (surface glycoprotein)",
            "start": 21563,
            "end": 25384,
            "row": 0,
            "color": "#accbe1"
        }
    ],
    "N": [
        {
            "name": "ORF10",
            "start": 29558,
            "end": 29674,
            "row": 0
        },
        {
            "name": "ORF1ab",
            "start": 266,
            "end": 21555,
            "row": 0
        },
        {
            "name": "ORF3a",
            "start": 25393,
            "end": 26220,
            "row": 0
        },
        {
            "name": "ORF6",
            "start": 27202,
            "end": 27387,
            "row": 0
        },
        {
            "name": "ORF7a",
            "start": 27394,
            "end": 27759,
            "row": 0
        },
        {
            "name": "ORF7b",
            "start": 27756,
            "end": 27887,
            "row": 0
        },
        {
            "name": "ORF8",
            "start": 27894,
            "end": 28259,
            "row": 0
        }
    ]
}


sars_cov_2_products = {
    "A": []
}

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
        gisaid_only = args.get('gisaid_only')

        filter_in = payload.get("gcm")
        q_type = payload.get("type")
        pairs = payload.get("kv")

        aa_only = True
        print("aa_only", aa_only)

        user_epitope_part = payload.get("userEpitope")
        if user_epitope_part is not None:
            epitope_json_part = gen_epitope_part_json_virusviz(user_epitope_part, filter_in=filter_in)

        epitope_without_variants_part = payload.get("epitope_without_variants")
        if epitope_without_variants_part is not None:
            epitope_json_part = gen_epitope_part_json_virusviz(epitope_without_variants_part, without_variants=True,
                                                               filter_in=filter_in)

        epitope_without_variants_part_all_population = payload.get("epitope_without_variants_all_population")
        if epitope_without_variants_part_all_population is not None:
            epitope_json_part = gen_epitope_part_json_virusviz(epitope_without_variants_part_all_population,
                                                               without_variants=True, all_population=True)

        epitope_part = payload.get("epitope")
        if epitope_part is not None:
            epitope_table = getMatView(filter_in['taxon_name'], epitope_part['product'])
            field_name = "toTable"
            epitope_json_part = gen_epitope_part_json_virusviz(epitope_part, filter_in=filter_in)
            epitope_part = gen_where_epi_query_field(epitope_part, field_name)
        else:
            epitope_table = None


        # # region Find virus information
        # if 'taxon_id' in filter_in and len(filter_in["taxon_id"]) == 1:
        #     the_virus = taxon_id_dict[filter_in['taxon_id'][0]]
        # elif 'taxon_name' in filter_in and len(filter_in["taxon_name"]) == 1:
        #     the_virus = taxon_name_dict[filter_in['taxon_name'][0].lower()]
        # else:
        #     the_virus = None
        #     flask.current_app.logger.debug(f"SINGLE VIRUS PROBLEM")
        #     api.abort(422, "Please select only one virus by using virus taxonomy name or taxonomy ID.")
        # reference_sequence_length = the_virus["nucleotide_sequence_length"]
        # taxon_id = the_virus["taxon_id"]
        # taxon_name = the_virus["taxon_name"]
        # n_products = the_virus["n_products"]
        # a_products = the_virus["a_products"]
        reference_sequence_length = 29903
        taxon_id = 2697049
        taxon_name = "severe acute respiratory syndrome coronavirus 2"
        n_products = sars_cov_2_products["N"]
        a_products = sars_cov_2_products["A"]
        # endregion

        poll_id = poll_cache.create_dict_element()

        def compress_sequence(sequence):
            seq_bytes = bytes(sequence, 'utf-8')
            compressed_seq = gzip.compress(seq_bytes)
            return base64.b64encode(compressed_seq).decode('utf-8')

        def async_function():
            try:
                res = list(full_query(filter_in, q_type, pairs, orderCol="sequence_id", limit=None, is_control=is_control,
                                 agg=False, orderDir="ASC", rel_distance=3, annotation_type=None, offset=0,
                                 gisaid_only=gisaid_only, epitope_part=epitope_part, epitope_table=epitope_table))

                res_sequence_id = [str(row["sequence_id"]) for row in res]

                if not aa_only:
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
                                    "schema": [] if aa_only else ["position", "from", "to", "type", ["effect", "putative_impact", "gene"]],
                                    "variants": [] if aa_only else res_nuc[row["sequence_id"]],
                                },
                                "A": {
                                    "schema": ["position", "from", "to", "type"],
                                    "variants": res_aa[row["sequence_id"]],
                                }
                            },
                            "sequenceFormat": "plain" if aa_only else "gzip",
                            "sequence": None if aa_only else compress_sequence(row["nucleotide_sequence"])
                        }
                    for row in res
                }

                result = {
                    'sequencesCount': len(sequences),
                    'taxon_id': taxon_id,
                    "exclude_n": aa_only,
                    "exclude_a": False,
                    # 'taxon_name': taxon_name,
                    # "referenceSequence": {"length": reference_sequence_length},
                    "schema": schema,
                    # "products": {
                    #     "A": a_products,
                    #     "N": n_products,
                    # },
                    "sequences": sequences
                }
                if not (epitope_part is None and user_epitope_part is None and epitope_without_variants_part is None
                        and epitope_without_variants_part_all_population is None):

                    result['epitopes'] = epitope_json_part

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
                flask.current_app.logger.error(e)
                poll_cache.set_result(poll_id, None)
                raise e

        from app import executor_inner
        executor_inner.submit(async_function)
        return flask.Response(json.dumps({'result': poll_id}), mimetype='application/json')

# Request URL: http://geco.deib.polimi.it/virusurf/api/query/table?is_control=false&page=1&num_elems=10&order_col=accession_id&order_dir=ASC
# Request Payload: {"gcm":{"taxon_name":["severe acute respiratory syndrome coronavirus 2"],"country":["italy"]},"type":"original","kv":{}}

# curl -X POST "http://localhost:5000/virusurf/api/viz/table?is_control=false" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"gcm\":{\"taxon_name\":[\"severe acute respiratory syndrome coronavirus 2\"],\"country\":[\"italy\"]},\"type\":\"original\",\"kv\":{}}"
