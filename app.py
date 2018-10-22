from flask import Flask, render_template

from apis import blueprint
from model.models import Biosample, db
from model.utils import column_dict
from utils import get_db_uri

app = Flask(__name__,
            static_folder='../vue-metadata/dist/static',
            template_folder='../vue-metadata/dist')

app.register_blueprint(blueprint)

app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
app.app_context().push()

app.logger.debug("testxx")

app.logger.debug(Biosample.query.first())
app.logger.debug("test2")

app.logger.debug(column_dict)


# Make a "catch all route" so all requests match our index.html file. This lets us use the new history APIs in the browser.
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def index(path):
    return render_template('index.html')


if __name__ == '__main__':
    app.run()
