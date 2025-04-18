import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DbConnection:
    """Class responsible for all implementations of database connection"""

    def run_query(self, query: str, get_all=False) -> list | None:
        """Method responsible for connect with database and execute queries

        :param query: The query to be run
        :type query: str
        :param get_all: The flag to return data from query
        :type get_all: bool
        :return None or query
        :rtype: None or str
        """
        db_connection = PostgresHook(
            postgres_conn_id=os.environ["DB_CONNECTION"],
            database=os.environ["DB_DATABASE"],
        )

        connection = db_connection.get_conn()

        if not connection:
            raise Exception("No connection with database")

        db_connection.set_autocommit(connection, True)

        db = connection.cursor()
        db.execute(query)

        if get_all:
            return db.fetchall()

        connection.close()
