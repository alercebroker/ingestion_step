from db_plugins.db.generic import DatabaseConnection
from db_plugins.db.mongo import MongoConnection
from db_plugins.db.sql import SQLConnection
from ingestion_step.utils.multi_driver.query import MultiQuery
from pymongo import MongoClient
from urllib.parse import quote_plus




class MultiDriverConnection(DatabaseConnection):
    def __init__(self, config: dict):

        username = quote_plus("elasticcadmin")
        password = quote_plus("aXU%mL%W1")
        client = MongoClient(
            f"mongodb://{username}:{password}@infra-elasticc-db.cluster-crmasfg8r2qb.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        )
        self.config = config
        self.psql_driver = SQLConnection()
        self.mongo_driver = MongoConnection(
            client=client,
            config= {
                "HOST": "infra-elasticc-db.cluster-crmasfg8r2qb.us-east-1.docdb.amazonaws.com",
                "USER": "elasticcadmin",
                "PASSWORD": "aXU%mL%W1",
                "PORT": 27017,
                "DATABASE": "alerts",
                "AUTH_SOURCE": "admin"
            }
        )
        self.mongo_driver = client["alerts"]

    def connect(self):
        self.mongo_driver.connect(self.config["MONGO"])
        self.psql_driver.connect(self.config["PSQL"])

    def create_db(self):
        self.mongo_driver.create_db()
        self.psql_driver.create_db()

    def drop_db(self):
        self.mongo_driver.drop_db()
        self.psql_driver.drop_db()
        self.psql_driver.session.close()

    def query(self, query_class=None, *args, **kwargs):
        return MultiQuery(
            self.psql_driver, self.mongo_driver, query_class, *args, **kwargs
        )
