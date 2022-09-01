"""Convert JSON files to CSV."""


import argparse
import csv
from src import file_io_utils as file_io


def main():
    """
    Driver method for creating CSV files from JSON input.

    Notes:
        1. When the file list of commits has a length
           longer than 1, the list is surrounded by quotes.
           This behavior was in the old extractor too.

        2.

    Output orders:
        "PR" file:
            "Issue_Number", "Issue_Title", "Issue_Author_Name",
            "Issue_Author_Login","Issue_Closed_Date", "Issue_Body",
            "Issue_Comments", "PR_Title", "PR_Author_Name",
            "PR_Author_Login", "PR_Closed_Date", "PR_Body",
            "PR_Comments", "Commit_Author_Name",
            "Commit_Date", "Commit_Message", "isPR"

        "Commit" file:
            "Issue_Num", "Author_Login", "File_Name",
            "Patch_Text", "Commit_Message", "Commit_Title"

    """
    cfg: dict = get_user_cfg()
    columns: list = cfg["columns"]
    issues_data: dict = file_io.read_jsonfile_into_dict(cfg["input_json"])

    with open(cfg["output_csv"], "w", newline="", encoding="utf-8") as out_csv:

        writer = csv.writer(
            out_csv,
            quoting=csv.QUOTE_MINIMAL,
            delimiter=cfg["delimiter"],
            quotechar='"',
            # escapechar="",
        )

        # write column labels
        writer.writerow(columns)

        for num, data in issues_data.items():
            # NOTE:
            # commit info all comes from the last commit right now
            _, last_commit = data["commits"].popitem()

            issue_col_tbl: dict = {
                "Issue_Num": num,
                "Issue_Title": data["title"],
                "Issue_Author_Name": data["userid"],
                "Issue_Author_Login": data["userlogin"],
                "Issue_Closed_Date": data["closed_at"],
                "Issue_Body": data["body"],
                "Issue_Comments": cfg["separator"].join(
                    [
                        comment["body"]
                        for _, comment in data["comments"].items()
                    ]
                ),
                # PR
                "Pr_Title": data["title"],
                "PR_Author_Name": data["userid"],
                "PR_Author_Login": data["userlogin"],
                "PR_Closed_Date": data["closed_at"],
                "PR_Body": data["body"],
                "isPR": data["is_pr"],
                # Commit
                "Commit_Author_Name": last_commit["author_name"],
                "Commit_Date": last_commit["date"],
                "Commit_Message": last_commit["message"],
                "File_Name": last_commit["files"]["file_list"],
                "Patch_Text": last_commit["files"]["patch_text"],
            }
            issue_col_tbl["PR_Comments"] = issue_col_tbl["Issue_Comments"]

            writer.writerow([issue_col_tbl[key] for key in columns])


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
        description="JSON ─► CSV",
    )

    # add repo input CLI arg
    arg_parser.add_argument(
        "extractor_cfg_file",
        help="Path to JSON configuration file",
    )

    return arg_parser.parse_args().extractor_cfg_file


if __name__ == "__main__":
    main()
