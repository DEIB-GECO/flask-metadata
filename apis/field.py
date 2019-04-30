import flask
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
import sqlalchemy

from model.models import t_flatten, db
from utils import columns_dict, run_query, unfold_list, sql_query_generator
from .flask_models import Info, info_field

api = Namespace('field',
                description='Operations related to fields (i.e., attributes from the Genomic Conceptual Model)')

field = api.model('Field', {
    'name': fields.String(attribute='column_name', required=True, description='Field name '),
    'group': fields.String(attribute='table_name', description='Field group '),
    'view': fields.String(attribute='view', description='Field view '),
    'description': fields.String(attribute='description', description='Field description '),
    'title': fields.String(attribute='title', description='Field title '),
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
        """List all available fields with description and belonging group"""
        res = columns_dict.values()
        res = list(res)
        age_item = [x for x in res if x.column_name == 'age'][0]
        res.pop(res.index(age_item))
        res.append(age_item)
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
# parser_body.add_argument('voc', type=inputs.boolean,
#                          help='Enable enriched search over controlled vocabulary terms and synonyms (true/false)',
#                          default=False)
parser_body.add_argument('body', type="json", help='json ', location='json')


@api.route("/age")
class Age(Resource):
    @api.expect(parser_body)
    def post(self):
        """For the posted query, returns minimum and maximum ages"""

        payload = api.payload
        filter_in = payload.get("gcm")
        pair_query = payload.get("kv")
        query = gen_query_field('age', 'original', filter_in, pair_query)
        res = db.engine.execute(query).fetchall()
        res = [row['label'] for row in res if row['label'] is not None]
        if res:
            result = {
                'max_age': max(res),
                'min_age': min(res)
            }
        else: result = {
                'max_age': "",
                'min_age': ""
            }

        return result



@api.route('/<field_name>')
@api.param('field_name', 'The requested field')
@api.response(404, 'Field not found')
class FieldValue(Resource):

    # still using for
    # def from_cypher(self, field_name, voc):
    #     column = columns_dict[field_name]
    #
    #     column_name = column.column_name
    #     table_name = column.table_name
    #     column_type = column.column_type
    #     has_tid = column.has_tid
    #
    #     # MATCH (n: Donor)
    #     # OPTIONAL MATCH(n)-[:HasTid{onto_attribute:'species'}]->(:Vocabulary)-->(s:Synonym)
    #     # UNWIND s.label + n.species as value
    #     # RETURN DISTINCT TOLOWER(value) as value
    #     # ORDER BY value
    #
    #     to_lower = 'TOLOWER' if column_type == str else ''
    #     cypher_query = f"MATCH (n: {table_name}) "
    #     if voc and has_tid:
    #         cypher_query += f"OPTIONAL MATCH(n)-[:HasTid{{onto_attribute:'{column_name}'}}]->(:Vocabulary)-->(s:Synonym) "
    #         cypher_query += f"UNWIND [s.label] + [n.{column_name}] as value "
    #     else:
    #         cypher_query += f"WITH n.{column_name} as value "
    #     cypher_query += f"RETURN DISTINCT {to_lower}(value) as value "
    #     cypher_query += "ORDER BY value"
    #
    #     flask.current_app.logger.info(cypher_query)
    #
    #     results = run_query(cypher_query)
    #     flask.current_app.logger.info('got results')
    #
    #     res = results.elements
    #
    #     # res has only one element in inner list, however I prefer to use general one
    #     res = unfold_list(res)
    #
    #     res = [{'value': x} for x in res]
    #
    #     info = Info(len(res), None)
    #
    #     res = {'values': res,
    #            'info': info
    #            }
    #     return res
    #
    # @api.doc('get_value_list')
    # @api.marshal_with(values)
    # @api.expect(parser)
    # def get(self, field_name):
    #     """For a specified field, it lists all possible values"""
    #
    #     args = parser.parse_args()
    #     voc = args['voc']
    #
    #     if field_name in columns_dict:
    #         return self.from_cypher(field_name, voc)
    #
    #     else:
    #         api.abort(404)

    @api.doc('post_value_list')
    @api.marshal_with(values)
    @api.expect(parser_body)
    def post(self, field_name):
        """For a specified field, it lists all possible values"""

        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pair_query = payload.get("kv")

        if field_name in columns_dict:
            column = columns_dict[field_name]
            column_name = column.column_name
            has_tid = column.has_tid
            if type == 'original':
                #     sql_column = t_flatten.c[column_name]
                #
                #     filter_in_new = {x: filter_in[x] for x in filter_in if x != column_name}
                #
                #     s = select([sql_column, func.count(t_flatten.c.item_id.distinct()).label('item_count')])
                #
                #     for (filter_column, filter_list) in filter_in_new.items():
                #         sql_filter_column = t_flatten.c[filter_column]
                #
                #         conditions = []
                #
                #         if None in filter_list:
                #             conditions.append(sql_filter_column.is_(None))
                #             filter_list = [x for x in filter_list if x is not None]
                #
                #             s = s.where(or_(sql_filter_column.in_(filter_list), sql_filter_column.is_(None)))
                # else:
                #
                # if len(filter_list):
                #     conditions.append(sql_filter_column.in_(filter_list))
                #
                # s = s.where(or_(*conditions))
                #
                # s = s.group_by(sql_column)
                # s = s.order_by(desc('item_count'), sql_column)
                #
                # select([t_flatten.c.item_id, t_flatten.c.biosample_type])
                # print(s)
                #
                query = gen_query_field(field_name, type, filter_in, pair_query)
                res = db.engine.execute(query).fetchall()

                item_count = sum(map(lambda row: row['item_count'], res))

                res = [{'value': row['label'], 'count': row['item_count']} for row in res]

                length = len(res)

                info = Info(length, length, item_count)

                res = {'values': res,
                       'info': info
                       }
                return res
            else:
                query = gen_query_field(field_name, type, filter_in, pair_query)
                print(query)
                res = db.engine.execute(query).fetchall()
                item_count = sum(map(lambda row: row['item_count'], res))
                res = [{'value': row['label'], 'count': row['item_count']} for row in res]

                length = len(res)
                info = Info(length, length, item_count)

                res = {'values': res,
                       'info': info
                       }
                return res
        else:
            api.abort(404)


def gen_query_field(field_name, type, filter_in, pair_query):
    column = columns_dict[field_name]
    column_name = column.column_name
    has_tid = column.has_tid
    if type == 'original':
        filter_in_new = {x: filter_in[x] for x in filter_in if x != column_name}
        sub_query1 = sql_query_generator(filter_in_new, pairs_query=pair_query, search_type=type,
                                         return_type='field_value', field_selected=field_name)
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
                                             return_type='field_value_tid', field_selected=field_name)
        select_part = f"SELECT label, count(*) as item_count "
        if has_tid:
            from_part = "FROM (" + sub_query1 + " union " + sub_query2 + ") as view"
        else:
            from_part = "FROM (" + sub_query1 + ") as view"
        group_by = " group by label "
        order_by = " order by item_count desc, label asc "
        query = select_part + from_part + group_by + order_by
        return sqlalchemy.text(query)
