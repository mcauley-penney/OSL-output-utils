"""
TODO.

Documentation:
    https://www.psycopg.org/docs/
"""

import psycopg2
from src.utils import file_io_utils as file_io


class PGCursor:
    """TODO."""

    def __init__(self, cfg: dict) -> None:
        """
        Initialize psycopg cursor object with configuration.

        Args:
            cfg (dict): JSON user cfg
        """
        self.cfg = cfg

        self.connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )

        self.cursor = self.connection.cursor()

        self.metrics_dict = file_io.read_jsonfile_into_dict(
            self.cfg["metrics_input"]
        )

    def write_changes_to_database(self) -> None:
        """Write changes made to database."""
        self.connection.commit()

    def close_database_connection(self) -> None:
        """Close connection to database."""
        for item in (self.cursor, self.connection):
            item.close()

    def update_keys_per_issue(self) -> None:
        """TODO."""
        metrics: dict = self.metrics_dict["per_issue"]
        keys: list = list(metrics.keys())

        cur_metrics: dict

        for key in keys:
            try:
                cur_metrics = metrics[key]

            except KeyError as err:
                print(f"ERROR: Key {key} cannot be accessed: {err}")

            else:
                self.__update_keys(key, cur_metrics)

    def update_keys_per_period(self) -> None:
        """TODO."""
        metrics: dict = self.metrics_dict["per_period"]

        for _, period_data in metrics.items():
            cur_metrics: dict = period_data.copy()
            del cur_metrics["keys"]

            for key in period_data["keys"]:
                self.__update_keys(key, cur_metrics)

    def __update_keys(self, key, metrics: dict):
        for metric_key, metric_val in metrics.items():
            self.update(metric_key, metric_val, str(key))

    def update(self, col: str, val: str, item_num: str):
        """Update a row in the desired database and table."""
        table = self.cfg["table"]

        update_query: str = (
            f"UPDATE {table} SET {col} = {val} WHERE pr = '{item_num}';"
        )

        self.cursor.execute(update_query)
