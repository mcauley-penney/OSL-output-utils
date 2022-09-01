"""TODO."""


import argparse
from src import postgres_utils
from src.utils import file_io_utils as file_io


def main():
    """Driver method for database update procedure."""
    cfg: dict = get_user_cfg()

    cursor = postgres_utils.PGCursor(cfg)

    cursor.update_keys_per_issue()

    cursor.update_keys_per_period()

    cursor.write_changes_to_database()

    cursor.close_database_connection()


def get_user_cfg() -> dict:
    """
    Get path to and read from configuration file.

    :return: dict of configuration values
    :rtype: dict
    """
    cfg_path = get_cli_args()

    return file_io.read_jsonfile_into_dict(cfg_path)


def get_cli_args() -> str:
    """
    Get initializing arguments from CLI.

    Returns:
        str: path to file with arguments to program
    """
    # establish positional argument capability
    arg_parser = argparse.ArgumentParser(
        description="Send JSON data to a Postgres database",
    )

    # add repo input CLI arg
    arg_parser.add_argument(
        "extractor_cfg_file",
        help="Path to configuration file",
    )

    return arg_parser.parse_args().extractor_cfg_file


if __name__ == "__main__":
    main()
