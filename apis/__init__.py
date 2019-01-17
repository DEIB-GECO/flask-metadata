from flask import Blueprint
from flask_restplus import Api

from .field import api as field_api
from .flask_models import api as models_api
from .query import api as query_api
from .item import api as item_api



# TODO change URL prefix before deploy
api_blueprint = Blueprint('api', __name__)

api = Api(api_blueprint,
          title='Repository Viewer API',
          version='1.0',
          description='Repository Viewer API is a set of RESTful endpoints -- programmable interfaces over the Web -- '
                      'that allows third-party developers to build automation scripts and apps. '
                      'This documentation describes the API in detail, including data model information of both inputs '
                      '(parameters) and outputs (response records). It also allows you to interact with and test out '
                      'each API directly on this page, which shall provide clear insights into how the API responds '
                      'to different parameters.'
                      '\nThe API allow to retrieve information about genomic experiments metadata, '
                      'as they are described by the [Genomic Conceptual Model](http://www.bioinformatics.deib.polimi.it/geco/publications/conceptual_modelling.pdf).'
                      '\nThey are divided in three parts:'
                      '\n* \'field\' allows to retrieve information about the fields for querying; '
                      '\n* \'query\' allows to perform queries with CYPHER language, inputing the text of the query as the body of a POST request;'
                      '\n* \'item\' allows to retrieve information, either in tabular or graph form of a specific item.',
          doc='/',
          )

api.add_namespace(models_api)

api.add_namespace(field_api)
api.add_namespace(query_api)
api.add_namespace(item_api)
