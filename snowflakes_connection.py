import traceback

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import conns


class SnowflakesConn:
    """
    This class is used for to make connection with snowflake and do CURD operations.
    """

    def __init__(self, logger):
        """
        This initialization the variables with the default value.
        Args:
            logger: logger object to add logs in file.
        """
        self.con_eb = None
        self.db_cursor_eb = None
        self.logger = logger

    def connection(self):
        """
        This function is used to create connection with snowflakes database.
        Returns:

        """
        try:
            self.con_eb = snowflake.connector.connect(
                user=conns["SnowflakeDB"]["UserName"],
                password=conns["SnowflakeDB"]["Password"],
                region=conns["SnowflakeDB"]["Region"],
                account=conns["SnowflakeDB"]["Account"],
                warehouse=conns["SnowflakeDB"]["Warehouse"],
                database=conns["SnowflakeDB"]["Database"],
                schema=conns["SnowflakeDB"]["Schema"],
                role=conns["SnowflakeDB"]["Role"],
                autocommit=conns["SnowflakeDB"]["AutoCommit"],
            )
            self.logger.info("Connected with snowflakes.")
            self.db_cursor_eb = self.con_eb.cursor()
            return True
        except Exception as err:
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())

    def create_table_channel(self):
        """
        This function creates YOUTUBE.PUBLIC.CHANNEL_VIDEOS table.
        Returns:

        """
        # CREATES TABLE CHANNEL_VIDEOS
        try:
            if self.connection():
                self.db_cursor_eb.execute("""create IF NOT EXISTS TABLE YOUTUBE.PUBLIC.CHANNEL_VIDEOS (
                                    CHANNEL_NAME VARCHAR(250),
                                    VIDEO_TITLE VARCHAR(500),
                                    VIDEO_LINK VARCHAR(500),
                                    VIDEO_DOWNLOAD_LINK VARCHAR(500),
                                    THUMBNAIL VARCHAR(500),
                                    "VIEWS" VARCHAR(100),
                                    UPLOAD_TIME VARCHAR(100)
                                );""")
                self.logger.info("CHANNEL_VIDEOS Table Created")
        except Exception as err:
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())

    def create_table_video(self):
        """
        This function creates YOUTUBE.PUBLIC.VIDEO_INFO table.
        Returns:

        """
        try:
            if self.connection():
                # CREATES TABLE VIDEO_INFO
                self.db_cursor_eb.execute("""create IF NOT EXISTS TABLE YOUTUBE.PUBLIC.VIDEO_INFO (
                                        VIDEO_LINK VARCHAR(500),
                                        VIDEO_TITLE VARCHAR(500),
                                        NO_OF_COMMENTS NUMBER(9,0),
                                        COMMENTER_NAME VARCHAR(500),
                                        LIKES NUMBER(9,0)
                                    );""")
                self.logger.info("VIDEO_INFO Table Created")
        except Exception as err:
            # self.driver.refresh()
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())

    def insert_data(self, ob, table_name):

        try:
            if self.connection():
                success, nchunks, nrows, _ = write_pandas(
                    conn=self.con_eb, df=ob, table_name=table_name, quote_identifiers=False
                )
                self.logger.info(f"Successfully inserted Data in {table_name}")
        except Exception as err:
            # self.driver.refresh()
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())

    def select_data(self, table_name, search_id=None):
        """
        This function is used to select data from tables.
        Args:
            table_name: Table Name
            search_id: Search String

        Returns:

        """
        # Execute a statement that will generate a result set.
        try:
            if self.connection():
                if search_id:
                    sql = f"select * from {table_name} where USERID = '{search_id}';"
                else:
                    sql = f"select * from {table_name};"
                self.logger.info(f"sql {sql}")
                self.db_cursor_eb.execute(sql)

                # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
                df = self.db_cursor_eb.fetch_pandas_all()
                return df
        except Exception as err:
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())

    def select_no_data(self, table_name, search_id=None, count=None):
        """
        This function is used to select particular count of data from tables.
        Args:
            table_name: Table Name
            search_id: Search String
            count: No of records need to select

        Returns:

        """
        try:
            self.connection()
            if search_id:
                sql = f"select top {count} * from {table_name} where USERID = '{search_id}';"
            else:
                sql = f"select count(*) from {table_name};"
            self.logger.info(f"sql {sql}")
            self.db_cursor_eb.execute(sql)

            # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
            df = self.db_cursor_eb.fetch_pandas_all()
            return df
        except Exception as err:
            self.logger.error("Error! " + str(err))
            self.logger.error(traceback.format_exc())
