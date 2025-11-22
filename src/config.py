import os
from dotenv import load_dotenv

load_dotenv()

class CouchbaseConfig:

    def __init__(self):
        self.host = os.getenv("COUCHBASE_HOST")
        self.port = os.getenv("COUCHBASE_PORT", 8091)
        self.user = os.getenv("COUCHBASE_USER", "Administrator")
        self.password = os.getenv("COUCHBASE_PASSWORD", "password")


class MongoDBConfig:

    def __init__(self, database_name: str=None):
        self.host = os.getenv("MONGODB_HOST", "localhost")
        self.port = os.getenv("MONGODB_PORT", 27017)
        self.user = os.getenv("MONGODB_USER", "admin")
        self.password = os.getenv("MONGODB_PASSWORD", "password")
        self.tls = os.getenv("MONGODB_TLS", "false")
        self.database = database_name if database_name else os.getenv("MONGODB_DB_NAME", "migra")
        
        _connection_string = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}"
        self.connection_string = os.getenv("MONGODB_CONNECTION_STRING", _connection_string)