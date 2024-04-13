from flask import Flask, json, request, Response, jsonify
from pymongo import MongoClient
from validate import validate_book, validate_email_and_password, validate_user
import jwt, os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

SECRET_KEY = os.environ.get("SECRET_KEY") or "this is a secret"
print(SECRET_KEY)
app.config["SECRET_KEY"] = "ecit_prithvi_12345"

from models import Document, User
from auth_middleware import token_required


@app.route("/users/", methods=["POST"])
def add_user():
    try:
        user = request.json
        if not user:
            return {
                "message": "Please provide user details",
                "data": None,
                "error": "Bad request",
            }, 400
        is_validated = validate_user(**user)
        if is_validated is not True:
            return dict(message="Invalid data", data=None, error=is_validated), 400
        user = User().create(**user)
        if not user:
            return {
                "message": "User already exists",
                "error": "Conflict",
                "data": None,
            }, 409
        return {"message": "Successfully created new user", "data": user}, 201
    except Exception as e:
        return {"message": "Something went wrong", "error": str(e), "data": None}, 500


@app.route("/users/login", methods=["POST"])
def login():
    try:
        data = request.json
        if not data:
            return {
                "message": "Please provide user details",
                "data": None,
                "error": "Bad request",
            }, 400
        # validate input
        is_validated = validate_email_and_password(
            data.get("email"), data.get("password")
        )
        if is_validated is not True:
            return dict(message="Invalid data", data=None, error=is_validated), 400
        user = User().login(data["email"], data["password"])
        if user:
            try:
                # token should expire after 24 hrs
                user["token"] = jwt.encode(
                    {"user_id": user["_id"]},
                    app.config["SECRET_KEY"],
                    algorithm="HS256",
                )
                return {"message": "Successfully fetched auth token", "data": user}
            except Exception as e:
                return {"error": "Something went wrong", "message": str(e)}, 500
        return {
            "message": "Error fetching auth token!, invalid email or password",
            "data": None,
            "error": "Unauthorized",
        }, 404
    except Exception as e:
        return {"message": "Something went wrong!", "error": str(e), "data": None}, 500


@app.route("/users/", methods=["GET"])
@token_required
def get_current_user(current_user):
    return jsonify(
        {"message": "successfully retrieved user profile", "data": current_user}
    )


@app.route("/users/", methods=["PUT"])
@token_required
def update_user(current_user):
    try:
        user = request.json
        if user.get("name"):
            user = User().update(current_user["_id"], user["name"])
            return (
                jsonify({"message": "successfully updated account", "data": user}),
                201,
            )
        return {
            "message": "Invalid data, you can only update your account name!",
            "data": None,
            "error": "Bad Request",
        }, 400
    except Exception as e:
        return (
            jsonify(
                {"message": "failed to update account", "error": str(e), "data": None}
            ),
            400,
        )


@app.route("/users/", methods=["DELETE"])
@token_required
def disable_user(current_user):
    try:
        User().disable_account(current_user["_id"])
        return jsonify({"message": "successfully disabled acount", "data": None}), 204
    except Exception as e:
        return (
            jsonify(
                {"message": "failed to disable account", "error": str(e), "data": None}
            ),
            400,
        )


@app.route("/postDocument", methods=["POST"])
@token_required
def add_document(current_user):
    data = request.json
    if data is None or data == {} or "Document" not in data:
        return Response(
            response=json.dumps({"Error": "Please provide connection information"}),
            status=400,
            mimetype="application/json",
        )
    obj1 = Document(data)
    response = obj1.write(data)
    return Response(
        response=json.dumps(response), status=200, mimetype="application/json"
    )


@app.route("/putDocument", methods=["PUT"])
@token_required
def update_document(current_user):
    data = request.json
    if data is None or data == {} or "DataToBeUpdated" not in data:
        return Response(
            response=json.dumps({"Error": "Please provide connection information"}),
            status=400,
            mimetype="application/json",
        )
    obj1 = Document(data)
    response = obj1.update()
    return Response(
        response=json.dumps(response), status=200, mimetype="application/json"
    )


@app.route("/getDocument", methods=["GET"])
@token_required
def read_document(current_user):
    data = request.json
    if data is None or data == {}:
        return Response(
            response=json.dumps({"Error": "Please provide connection information"}),
            status=400,
            mimetype="application/json",
        )
    obj1 = Document(data)
    response = obj1.read()
    return Response(
        response=json.dumps(response), status=200, mimetype="application/json"
    )


@app.route("/deleteDocument", methods=["DELETE"])
@token_required
def delete_document(current_user):
    data = request.json
    if data is None or data == {} or "Filter" not in data:
        return Response(
            response=json.dumps({"Error": "Please provide connection information"}),
            status=400,
            mimetype="application/json",
        )
    obj1 = Document(data)
    response = obj1.delete(data)
    return Response(
        response=json.dumps(response), status=200, mimetype="application/json"
    )


@app.errorhandler(403)
def forbidden(e):
    return jsonify({"message": "Forbidden", "error": str(e), "data": None}), 403


@app.errorhandler(404)
def forbidden(e):
    return (
        jsonify({"message": "Endpoint Not Found", "error": str(e), "data": None}),
        404,
    )


if __name__ == "__main__":
    app.run()
