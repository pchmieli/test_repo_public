#
# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import os

from collections import namedtuple

from flask import Flask, jsonify
from flask_swagger import swagger
from impala import dbapi
from version import VERSION

logging.basicConfig(level=logging.INFO)

DEFAULT_APP_PORT = '5000'
INVALIDATE_METADATA = 'INVALIDATE METADATA'
REFRESH = 'REFRESH'
DEFAULT_ROUTE_PARAMS = {
    "database": None,
    "table": None,
}

Configuration = namedtuple("Configuration", "impala_host impala_port")

app = Flask(__name__)

def load_configuration():
    return Configuration(
        os.environ['IMPALA_HOST'],
        int(os.environ['IMPALA_PORT']),
    )

@app.route("/", methods=["GET"])
def check_health():
    """
    Service health check
    ---
    responses:
        200:
            description: Service is running OK
    """
    return 'Service running OK'

def execute_impala_command(command, *args, **kwargs):
    with dbapi.connect(host=config.impala_host, port=config.impala_port) as conn:
        cursor = conn.cursor()
        cursor.execute(command, *args, **kwargs)
        return cursor


def run_scoped_command(command, table, message):
    command += " " + table
    try:
        execute_impala_command(command)
        return message % (table,)
    except Exception as ex:
        logging.exception("Failed executing: %s", command)
        return "Error processing command: %s: %s" % (command, str(ex))


@app.route("/invalidate", methods=["POST"])
@app.route("/invalidate/<table>", methods=["POST"])
def invalidate_metadata(table=""):
    """
    Invalidate metastore
    ---
    parameters:
        - in: path
          name: table
          required: false
          type: string
    responses:
        200:
            description: Metastore successfully invalidated
    """
    return run_scoped_command(INVALIDATE_METADATA, table,
                              "Metastore invalidated %s successfully")


@app.route("/refresh/<table>", methods=["POST"])
def refresh_table(table):
    """
    Invalidate table in metastore
    ---
    parameters:
        - in: path
          name: table
          required: false
          type: string
    responses:
        200:
            description: Table successfully invalidated
    """
    return run_scoped_command(REFRESH, table,
                              "Refresh %s successful")


@app.route("/api/spec", methods=["GET"])
def spec():
    swag = swagger(app)
    swag['info']['version'] = VERSION
    swag['info']['title'] = "Metastore refresh service api"
    return jsonify(swag)



if __name__ == "__main__":
    config = load_configuration()
    app.run(
        host=os.getenv('HOST', '0.0.0.0'),
        port=int(os.getenv('VCAP_APP_PORT', DEFAULT_APP_PORT)),
        use_reloader=False)
