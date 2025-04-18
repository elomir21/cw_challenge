from typing import Any


class Convert:
    """Class responsible for all implementations of convert"""

    @classmethod
    def db_insert_data_format(cls, db_insert_data: Any) -> Any:
        """Method responsible for format the data received to database pattern"""
        if db_insert_data is None:
            return "NULL"
        elif isinstance(db_insert_data, bool):
            return db_insert_data

        return f"'{db_insert_data}'"
