import datetime

import flask
import sqlalchemy
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs

from model.models import db
from utils import columns_dict, columns_dict_all, sql_query_generator
from .flask_models import Info, info_field

api = Namespace('db_info',
                description='Operations related to info')

db_info_source = api.model('DbInfoSource', {
    'taxon_id': fields.Integer,
    'taxon_name': fields.String,
    'database_source': fields.String,
    'update_date': fields.String,
})

db_info = api.model('DbInfo', {
    'update_date': fields.String,
    'sources': fields.List(fields.Nested(db_info_source)),
})


@api.route('')
class DbInfo(Resource):
    @api.doc('get_db_info')
    @api.marshal_with(db_info, skip_none=True)
    def get(self):
        """List all available fields with description and the group they belong to"""
        print("hello")
        query_text = "SELECT taxon_id, taxon_name, source as database_source, date_of_import as update_date FROM db_meta NATURAL JOIN virus"
        res = db.engine.execute(sqlalchemy.text(query_text))
        res = [dict(r) for r in res]
        res = {'sources': res}
        return res
