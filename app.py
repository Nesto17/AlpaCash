from flask import Flask, request, render_template, make_response
from flask_restful import Api, Resource
from flask_cors import CORS

from dotenv import load_dotenv
import os
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.serialize import model_to_json, model_from_json
import plotly.graph_objs as go
import boto3

load_dotenv()

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_bucket = 's3-alpaca-stock-data'
s3_model_prefix = 'prophet-models'

app = Flask(__name__)
api = Api(app)
CORS(app, resources={r"/*": {"origins": "*"}})

class Index(Resource):
    def get(self):
        headers = {'Content-Type': 'text/html'}
        return make_response(render_template('base.html'), 200, headers)

class Forecast(Resource):
    def get(self, ticker):
        ticker = ticker.upper()
        session = boto3.Session(
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = 'us-west-1'
        )
        s3 = session.client('s3')

        try:
            res = s3.get_object(Bucket=s3_bucket, Key=f'{s3_model_prefix}/{ticker}_model.json')
            m = model_from_json(res['Body'].read().decode('utf-8'))

            future = m.make_future_dataframe(periods=365)
            forecast = m.predict(future)

            fig = plot_plotly(m, forecast).to_html(full_html=False, 
                        config={'displayModeBar': False, 'responsive': True},
                        include_plotlyjs='cdn',
                        default_width='100%',
                        default_height='100%')

            return {"html": fig}, 200
        except:
            return {"error_message": "Error 404... *sad noises*"}, 400
    

api.add_resource(Index, '/')
api.add_resource(Forecast, '/forecast/<string:ticker>')

if __name__ == '__main__':
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(debug=True, host='0.0.0.0')