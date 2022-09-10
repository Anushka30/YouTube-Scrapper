import traceback

import pymongo


class MongoDBConnect:
    """
    This class is used to create mongodb client and perform create, insert and close client operations.
    """

    def __init__(self, username, password, logger):
        """
        This function is used to connect to Mongodb.
        """
        try:
            self.logger = logger
            self.url = (
                f"mongodb+srv://{username}:{password}@anushka1008.4zzw3dy.mongodb.net/?retryWrites=true&w"
                f"=majority"
            )
        except Exception as err:
            self.logger.error(
                f"(__init__): Something went wrong on initiation process\n {err}"
            )
            self.logger.error(traceback.format_exc())

    def get_mongo_db_client_object(self):
        """
        This function creates the client object for connection purpose
        """
        try:
            mongo_client = pymongo.MongoClient(self.url)
            self.logger.info("mongo_client created.")
            return mongo_client
        except Exception as err:
            self.logger.error(
                f"(get_mongo_db_client_object): Failed to create of client object\n {err}"
            )
            self.logger.error(traceback.format_exc())

    def get_database(self, db_name):
        """
        This function is used to return databases.
        """
        try:
            mongo_client = self.get_mongo_db_client_object()
            mongo_client.close()
            self.logger.info("mongo_client connection closed.")
            return mongo_client[db_name]
        except Exception as err:
            self.logger.error(
                f"(get_database): Failed to get the database list\n {err}"
            )
            self.logger.error(traceback.format_exc())

    def get_collection(self, collection_name, db_name):
        """
        This function is used to return collection.
        :return:
        """
        try:
            database = self.get_database(db_name)
            return database[collection_name]
        except Exception as err:
            self.logger.error(
                f"(get_collection): Failed to get the collection list\n {err}"
            )
            self.logger.error(traceback.format_exc())

    def is_collection_present(self, collection_name, db_name):
        """
        This function is used to check if collection is present or not.
        :param collection_name: Collection Name
        :param db_name: Database Name
        :return: Boolean
        """
        try:
            database_status = self.is_database_present(db_name=db_name)
            if database_status:
                database = self.get_database(db_name=db_name)
                if collection_name in database.list_collection_names():
                    self.logger.info(f"collection {collection_name} is present.")
                    return True
                else:
                    return False
            else:
                return False
        except Exception as err:
            self.logger.error(
                f"(is_collection_present): Failed to check collection\n {err}"
            )
            self.logger.error(traceback.format_exc())

    def is_database_present(self, db_name):
        """
        This function is used to check if the database is present or not.
        :param db_name: Database Name
        :return:
        """
        try:
            mongo_client = self.get_mongo_db_client_object()
            if db_name in mongo_client.list_database_names():
                self.logger.info(f"database {db_name} is present.")
                mongo_client.close()
                self.logger.info("mongo_client connection closed.")
                return True
            else:
                mongo_client.close()
                return False
        except Exception as err:
            self.logger.error(
                f"(is_database_present): Failed on checking if the database is present or not \n {err}"
            )
            self.logger.error(traceback.format_exc())

    def create_database(self, db_name):
        """
        This function is used to create database.
        :param db_name: Database Name
        :return:
        """
        try:
            database_check_status = self.is_database_present(db_name=db_name)
            if not database_check_status:
                mongo_client = self.get_mongo_db_client_object()
                database = mongo_client[db_name]
                self.logger.info(f"Database {db_name} created.")
                mongo_client.close()
                return database
            else:
                mongo_client = self.get_mongo_db_client_object()
                database = mongo_client[db_name]
                mongo_client.close()
                return database
        except Exception as err:
            self.logger.error(
                f"(create_database): Failed on creating database \n {err}"
            )
            self.logger.error(traceback.format_exc())

    def create_collection(self, collection_name, db_name):
        """
        This function is used to create the collection in the database given.
        :param collection_name: Collection Name
        :param db_name: Database Name
        :return:
        """
        try:
            collection_check_status = self.is_collection_present(
                collection_name=collection_name, db_name=db_name
            )
            if not collection_check_status:
                database = self.get_database(db_name=db_name)
                collection = database[collection_name]
                self.logger.info(f"Collection {collection_name} created.")
                return collection
        except Exception as err:
            self.logger.error(
                f"(create_collection): Failed to create collection \n {err}"
            )
            self.logger.error(traceback.format_exc())

    def insert_records(self, db_name, collection_name, records):
        """
        This function is used to insert records.
        :param db_name: Database Name
        :param collection_name: Collection Name
        :param records: Record
        :return:
        """
        try:
            record = []
            collection_check_status = self.is_collection_present(
                collection_name=collection_name, db_name=db_name
            )
            collection = self.get_collection(
                collection_name=collection_name, db_name=db_name
            )
            if collection_check_status:
                x = collection.find_one({"VIDEO_TITLE": records["VIDEO_TITLE"]})
                if x:
                    self.logger.info(f"{x['_id']}.")
                    if len(x["COMMENTS"]) != len(records["COMMENTS"]):
                        myquery = {"_id": x["_id"]}
                        newvalues = {"$set": {"COMMENTS": records["COMMENTS"]}}
                        x = collection.update_many(myquery, newvalues)
                        self.logger.info(f"{x.modified_count} documents updated.")
                    self.logger.info("Already documents updated.")
                else:
                    record.append(records)
                    collection.insert_many(record)
                    self.logger.info(f"Data inserted in {collection_name}.")

            else:
                self.create_database(db_name=db_name)
                self.create_collection(collection_name=collection_name, db_name=db_name)
                record.append(records)
                collection.insert_many(record)
                self.logger.info(f"Data inserted in {collection_name}.")
        except Exception as err:
            self.logger.error(traceback.format_exc())
            raise Exception(f"(insert_records): Failed to insert records \n {err}")

    def find_records(self, db_name, collection_name, title):
        """
        This function is used to find records in database.
        Args:
            db_name: Database Name
            collection_name: Collection Name
            title: Video title

        Returns:
        """
        try:
            collection_check_status = self.is_collection_present(
                collection_name=collection_name, db_name=db_name
            )
            if collection_check_status:
                collection = self.get_collection(
                    collection_name=collection_name, db_name=db_name
                )
                data = collection.find(
                    {"VIDEO_TITLE": title}, {"_id": 0, "VIDEO_TITLE": 1, "COMMENTS": 1}
                )
                self.logger.info(f"Record found in {collection_name}.")
                return data
        except Exception as err:
            self.logger.error(
                f"(find_records): Failed to find record for the given collection and database \n {err}"
            )
            self.logger.error(traceback.format_exc())
