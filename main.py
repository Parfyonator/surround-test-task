from exonum_client.api import ServiceApi, PublicApi
from exonum_client.message import ExonumMessage
from json import dumps
from logging import getLogger
from sqlalchemy import create_engine, inspect
from sanic import Sanic
from sanic.response import json


logger = getLogger(__name__)


engine = create_engine("postgres://postgres:lht,tltym@127.0.0.1:5432/surround_test")
connection = engine.connect()
# inspector = inspect(engine)
# schema_name = "public"
# print("Tables:", inspector.get_table_names(schema=schema_name))


def insert_contract(public_key, contract):
    engine.execute(f'INSERT INTO "Contracts" (public_key, contract) VALUES ({public_key}, {contract})')


def insert_token(public_key, token):
    engine.execute(f'INSERT INTO "Tokens" (public_key, token) VALUES ({public_key}, {token})')


public_api = PublicApi("127.0.0.1", "8080", "http")
timestamping_api = ServiceApi("timestamping", "127.0.0.1", "8080", "http")
app = Sanic()


def get_artifact_info(request):
    # get available services
    resp = public_api.available_services()
    if resp.status_code != 200:
        err_msg = "Unable to retrieve services."
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    available_services = resp.json()["services"]

    # get artifact name and version of timestamping service
    for service in available_services:
        if "timestamping" in service["spec"]["name"]:
            artifact_name = service["spec"]["artifact"]["name"]
            artifact_version = service["spec"]["artifact"]["version"]

    return artifact_name, artifact_version


@app.route("/create_token_contract")
async def create_token_contract(request):
    # get artifact info
    artifact_name, artifact_version = get_artifact_info(request)

    # receive data
    data = request.json
    if "tx_body" not in data:
        err_msg = f"Invalid request: {data}"
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    tx_body = data["tx_body"]

    # message from hex
    message = ExonumMessage.from_hex(tx_body, artifact_name, artifact_version, "TxCreateTokenContract")

    # get public key
    public_key = message.owner

    resp = timestamping_api.post_service("v1/token_contracts/value", dumps({"tx_body": tx_body}))

    if resp.status_code != 200:
        err_msg = "Unsuccessfully attempted to create token"
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    insert_token(public_key, resp.json())

    return json({"status": "success"})


@app.route("/create_tokens")
async def create_tokens(request):
    # get artifact info
    artifact_name, artifact_version = get_artifact_info(request)

    # receive data
    data = request.json
    if "tx_body" not in data:
        err_msg = f"Invalid request: {data}"
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    tx_body = data["tx_body"]

    # message from hex
    message = ExonumMessage.from_hex(tx_body, artifact_name, artifact_version, "TxCreateTokens")

    # get public key
    public_key = message.owner

    resp = timestamping_api.post_service("v1/tokens/value", dumps({"tx_body": tx_body}))

    if resp.status_code != 200:
        err_msg = "Unsuccessfully attempted to create token"
        logger.critical(err_msg)
        raise RuntimeError(err_msg)

    insert_token(public_key, resp.json())

    return json({"status": "success"})


@app.route("/transfer_token")
async def transfer_token(request):
    print("Request body:", request.json)
    return json({"status": "success"})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
