"""Application Models"""
import bson, os
from dotenv import load_dotenv
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash

load_dotenv()

DATABASE_URL = "mongodb://localhost:27017/amazon_marketplace_scraping"
client = MongoClient(DATABASE_URL)
db = client.amazon_marketplace_scraping


class Document:
    """Document Model"""

    def __init__(self, data):
        self.client = MongoClient("mongodb://localhost:27017/")
        database = data["database"]
        collection = data["collection"]
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

    def write(self, data):
        new_document = data["Document"]
        response = self.collection.insert_one(new_document)
        output = {
            "Status": "Successfully Inserted",
            "Document_ID": str(response.inserted_id),
        }
        return output

    def update(self):
        filt = self.data["Filter"]
        updated_data = {"$set": self.data["DataToBeUpdated"]}
        response = self.collection.update_one(filt, updated_data)
        output = {
            "Status": "Successfully Updated"
            if response.modified_count > 0
            else "Nothing was updated."
        }
        return output

    def read(self):
        filt = self.data["Filter"]
        document = self.collection.find_one(filt)
        output = [{item: document[item] for item in document if item != "_id"}]
        return output

    def delete(self):
        filt = self.data["Filter"]
        response = self.collection.delete_one(filt)
        output = {
            "Status": "Successfully Deleted"
            if response.deleted_count > 0
            else "Document not found."
        }
        return output


class User:
    """User Model"""

    def __init__(self):
        return

    def create(self, name="", email="", password=""):
        """Create a new user"""
        user = self.get_by_email(email)
        if user:
            return
        new_user = db.users.insert_one(
            {
                "name": name,
                "email": email,
                "password": self.encrypt_password(password),
                "active": True,
            }
        )
        return self.get_by_id(new_user.inserted_id)

    def get_all(self):
        """Get all users"""
        users = db.users.find({"active": True})
        return [{**user, "_id": str(user["_id"])} for user in users]

    def get_by_id(self, user_id):
        """Get a user by id"""
        user = db.users.find_one({"_id": bson.ObjectId(user_id), "active": True})
        if not user:
            return
        user["_id"] = str(user["_id"])
        user.pop("password")
        return user

    def get_by_email(self, email):
        """Get a user by email"""
        user = db.users.find_one({"email": email, "active": True})
        if not user:
            return
        user["_id"] = str(user["_id"])
        return user

    def update(self, user_id, name=""):
        """Update a user"""
        data = {}
        if name:
            data["name"] = name
        user = db.users.update_one({"_id": bson.ObjectId(user_id)}, {"$set": data})
        user = self.get_by_id(user_id)
        return user

    def delete(self, user_id):
        """Delete a user"""
        Books().delete_by_user_id(user_id)
        user = db.users.delete_one({"_id": bson.ObjectId(user_id)})
        user = self.get_by_id(user_id)
        return user

    def disable_account(self, user_id):
        """Disable a user account"""
        user = db.users.update_one(
            {"_id": bson.ObjectId(user_id)}, {"$set": {"active": False}}
        )
        user = self.get_by_id(user_id)
        return user

    def encrypt_password(self, password):
        """Encrypt password"""
        return generate_password_hash(password)

    def login(self, email, password):
        """Login a user"""
        user = self.get_by_email(email)
        if not user or not check_password_hash(user["password"], password):
            return
        user.pop("password")
        return user
