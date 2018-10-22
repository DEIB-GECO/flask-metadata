from flask_restplus import Api
from flask import Blueprint

from .value import api as value_api

# TODO change URL prefix before deploy
blueprint = Blueprint('api', __name__, url_prefix='/api')

api = Api(blueprint,
          title='Metadata API',
          version='1.0',
          description='An API contains Metadata operations', )

api.add_namespace(value_api)
