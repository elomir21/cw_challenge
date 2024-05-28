import requests
from concurrent.futures import ThreadPoolExecutor
from utils.db_connection import DbConnection


class DataExtract:
    """Class responsible for all implementations of data extraction of the chalange
    """

    @classmethod
    def formatting_data(cls):
        """Method responsible to format data"""
        remove_none = """
            UPDATE gdp
            SET value = null
            WHERE value = 'None'
        """
        db = DbConnection()
        db.run_query(remove_none)

    
    @classmethod
    def generate_report(cls, country_ids):
        """Method responsible for generate the report required"""
        db = DbConnection()
        for country_id in country_ids:
            report_insert = f"""
                INSERT INTO report
                SELECT
                    c.country_id,
                    c.name,
                    c.iso3_code,
                    (select value from gdp where year = '2019' and country_id = '{country_id}'):: decimal as "2019",
                    (select value from gdp where year = '2020' and country_id = '{country_id}'):: decimal as "2020",
                    (select value from gdp where year = '2021' and country_id = '{country_id}'):: decimal as "2021",
                    (select value from gdp where year = '2022' and country_id = '{country_id}'):: decimal as "2022",
                    (select value from gdp where year = '2023' and country_id = '{country_id}'):: decimal as "2023"
                FROM country c
                JOIN gdp g on c.country_id = g.country_id
                WHERE c.country_id = '{country_id}'
                GROUP BY
                    c.country_id,
                    c.name,
                    c.iso3_code;
            """
            print(report_insert)
            db.run_query(report_insert)


    @classmethod
    def get_api_data(cls):
        """Method responsible for get the data from api"""
        
        countries_available = ["ARG", "BOL", "BRA", "CHL", "COL", "ECU", "GUY", "PRY", "PER", "SUR", "URY", "VEN"]

        url = f"https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json&page=1&per_page=1000"

        response = requests.get(url)

        api_data = response.json()

        countries_data = []

        for data in api_data[1]:
            countries_data.append(
                (
                    {
                        "country_id": data.get("country").get("id", None),
                        "country_name": data.get("country").get("value", None),
                        "iso3_code": data.get("countryiso3code", None),
                        "year": data.get("date", None),
                        "value": data.get("value", None)
                    }
                )
            )

        if countries_data:
            cls.insert_data(countries_data)


    @classmethod
    def insert_data(cls, countries_data):
        """Method responsible for insert data into db

        :param country_data: Country informations
        :type country_data: list
        """
        db = DbConnection()

        country_ids = []

        for country_data in countries_data:
            country_insert = f"""
                INSERT INTO country VALUES (
                    '{country_data["country_id"]}',
                    '{country_data["country_name"]}',
                    '{country_data["iso3_code"]}'
                );
            """

            if country_data["country_id"] not in country_ids:
                print("country inserted")
                db.run_query(country_insert)
                country_ids.append(country_data["country_id"]) if country_data["country_id"] not in country_ids else None

            gdp_insert = f"""
                INSERT INTO gdp VALUES (
                    '{country_data["country_id"]}',
                    '{country_data["year"]}',
                    '{country_data["value"]}'
                );
            """
            db.run_query(gdp_insert)

        cls.formatting_data()
        cls.generate_report(country_ids)

