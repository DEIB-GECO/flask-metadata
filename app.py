from flask import Flask, render_template

from apis import blueprint
from model.utils import column_dict
from model.models import Biosample, db
from utils import get_db_uri

app = Flask(__name__)

app.register_blueprint(blueprint)

app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
app.app_context().push()

app.logger.debug("testxx")

app.logger.debug(Biosample.query.first())
app.logger.debug("test2")

app.logger.debug(column_dict)

if __name__ == '__main__':
    app.run()



