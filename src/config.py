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

    def __init__(self):
        self.host = os.getenv("MONGODB_HOST", "localhost")
        self.port = os.getenv("MONGODB_PORT", 27017)
        self.user = os.getenv("MONGODB_USER", "admin")
        self.password = os.getenv("MONGODB_PASSWORD", "password")
        self.db = os.getenv("MONGODB_DB_NAME", "migra")
        self.tls = os.getenv("MONGODB_TLS", "false")