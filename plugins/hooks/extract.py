import logging
import requests
from db.db_connection import DbConnection
from utils.functional import Convert


class DataExtract:
    """Class responsible for all implementations of data extraction of the chalange"""

    @classmethod
    def generate_report(cls) -> None:
        """Method responsible for generate the report required"""
        logging.info("Generating report")

        db = DbConnection()

        query_country_ids = """
            SELECT 
                country_id 
            FROM country;
        """

        results = []
        try:
            results = db.run_query(query_country_ids, get_all=True)
        except Exception as e:
            logging.error(f"Error to select country data from database: {e}")
            logging.error(query_country_ids)

        if not results:
            return

        logging.info("Generating report")

        country_ids = [country_id[0] for country_id in results]

        for country_id in country_ids:
            report_insert = f"""
                INSERT INTO report
                SELECT
                    c.country_id,
                    c.name,
                    c.iso3_code,
                    (select value from gdp where year = '2019' and country_id = {Convert.db_insert_data_format(country_id)}):: decimal as "2019",
                    (select value from gdp where year = '2020' and country_id = {Convert.db_insert_data_format(country_id)}):: decimal as "2020",
                    (select value from gdp where year = '2021' and country_id = {Convert.db_insert_data_format(country_id)}):: decimal as "2021",
                    (select value from gdp where year = '2022' and country_id = {Convert.db_insert_data_format(country_id)}):: decimal as "2022",
                    (select value from gdp where year = '2023' and country_id = {Convert.db_insert_data_format(country_id)}):: decimal as "2023"
                FROM country c
                JOIN gdp g on c.country_id = g.country_id
                WHERE c.country_id = {Convert.db_insert_data_format(country_id)}
                GROUP BY
                    c.country_id,
                    c.name,
                    c.iso3_code
                ON CONFLICT DO NOTHING;
            """

            try:
                db.run_query(report_insert)
            except Exception as e:
                logging.error(f"Error to insert report data from database: {e}")
                logging.error(report_insert)

    @classmethod
    def insert_data(cls, countries_data: list) -> None:
        """Method responsible for insert data into db

        :param country_data: Country informations
        :type country_data: list
        """
        logging.info("Inserting data into database")

        db = DbConnection()

        for country_data in countries_data:
            country_insert = f"""
                INSERT INTO country VALUES (
                    {Convert.db_insert_data_format(country_data["country_id"])},
                    {Convert.db_insert_data_format(country_data["country_name"])},
                    {Convert.db_insert_data_format(country_data["iso3_code"])}
                ) ON CONFLICT DO NOTHING;
            """

            try:
                db.run_query(country_insert)
            except Exception as e:
                logging.error(f"Error to insert country data from database: {e}")
                logging.error(country_insert)

            gdp_insert = f"""
                INSERT INTO gdp VALUES (
                    {Convert.db_insert_data_format(country_data["country_id"])},
                    {Convert.db_insert_data_format(country_data["year"])},
                    {Convert.db_insert_data_format(country_data["value"])}
                ) ON CONFLICT DO NOTHING;
            """

            try:
                db.run_query(gdp_insert)
            except Exception as e:
                logging.error(f"Error to insert gdp data from database: {e}")
                logging.error(gdp_insert)

        cls.generate_report()

    @classmethod
    def get_api_data(cls) -> None:
        """Method responsible for get the data from api"""
        logging.info("Getting data from API")

        countries_available = [
            "ARG",
            "BOL",
            "BRA",
            "CHL",
            "COL",
            "ECU",
            "GUY",
            "PRY",
            "PER",
            "SUR",
            "URY",
            "VEN",
        ]

        requests_data = {
            "host_url": "https://api.worldbank.org",
            "endpoint": f"/v2/country/{';'.join(countries_available)}/indicator/NY.GDP.MKTP.CD",
            "params": {"format": "json", "page": 1, "per_page": "100"},
        }

        response = requests.get(
            url=requests_data["host_url"] + requests_data["endpoint"],
            params=requests_data["params"],
        )

        api_data = response.json()

        countries_data = []

        while api_data[0].get("page") <= api_data[0].get("pages"):
            requests_data["params"]["page"] += 1

            response = requests.get(
                url=requests_data["host_url"] + requests_data["endpoint"],
                params=requests_data["params"],
            )

            api_data = response.json()

            for data in api_data[1]:
                countries_data.append(
                    {
                        "country_id": data.get("country").get("id", None),
                        "country_name": data.get("country").get("value", None),
                        "iso3_code": data.get("countryiso3code", None),
                        "year": data.get("date", None),
                        "value": data.get("value", None),
                    }
                )

            if not countries_data:
                return

            cls.insert_data(countries_data)
