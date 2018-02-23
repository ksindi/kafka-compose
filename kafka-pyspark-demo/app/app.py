# -*- coding: utf-8 -*-
"""Flask backend app init."""

from flask import Flask
from flask_cors import CORS
from flask_restful import Api, reqparse, Resource

API_BASE_URL = '/api/v1'  # config variable

parser = reqparse.RequestParser()


class Tweet(Resource):
    def get(self, tweet_id):
        print("Call for GET /tweet")
        parser.add_argument('q')
        query_string = parser.parse_args()
        q = query_string['q']
        return {}


def create_app():
    app = Flask(__name__)
    CORS(app)
    api = Api(app)
    api.add_resource(Tweet, API_BASE_URL + '/tweet/<tweet_id>')
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', debug=True, threaded=True)
