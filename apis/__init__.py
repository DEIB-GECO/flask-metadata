from flask import Blueprint
from flask_restplus import Api

from .field import api as field_api
from .flask_models import api as models_api
from .query import api as query_api
from .item import api as item_api



# TODO change URL prefix before deploy
api_blueprint = Blueprint('api', __name__)

api = Api(api_blueprint,
          title='Metadata API',
          version='1.0',
          description='An API contains Metadata operations',
          doc='/',
          )

api.add_namespace(models_api)

api.add_namespace(field_api)
api.add_namespace(query_api)
api.add_namespace(item_api)
