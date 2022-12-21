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
    columns = get_columns(cfg["output_type"])
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
            row_data = collect_row_data(num, data, cfg["separator"])
            writer.writerow([row_data[key] for key in columns])


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
        description="JSON → CSV",
    )

    # add repo input CLI arg
    arg_parser.add_argument(
        "extractor_cfg_file",
        help="Path to JSON configuration file",
    )

    return arg_parser.parse_args().extractor_cfg_file


def get_columns(output_type: str) -> list:
    """
    Return columns used to organize CSV output.

    Args:
        output_type (str): type of output to return. Can be "pr" or anything
                           else.

    Returns:
        list: list of CSV column names as strings

    """
    cols: dict = {
        # deprectated?
        # "commit": [
        #     "Issue_Num",
        #     "Author_Login",
        #     "File_Names",
        #     "Patch_Text",
        #     "Commit_Message",
        #     "Commit_Title",
        # ],
        # "pr": [
        #     "Issue_Num",
        #     "Issue_Title",
        #     "Issue_Author_ID",
        #     "Issue_Author_Login",
        #     "Issue_Closed_Date",
        #     "Issue_Body",
        #     "Issue_Comments",
        #     "PR_Title",
        #     "PR_Author_Name",
        #     "PR_Author_Login",
        #     "PR_Closed_Date",
        #     "PR_Body",
        #     "PR_Comments",
        #     "Commit_Author_Name",
        #     "Commit_Date",
        #     "Commit_Message",
        #     "isPR",
        # ],
        ##########################

        # NOTE: ETL1 inputs
        "merged_closed_commits": [
            "Author_Login",
            "Committer login",
            "Issue_Num",
            "SHA",
            "Commit_Message",
            "File_Names",  # equivalent to file_list of last commit
            "Patch_Text",
            "Status",
            "Additions",
            "Deletions",
            "Changes",
        ],
        "merged_closed_pulls": [
            "Issue_Num",
            "Issue_Author_Login",
            "Issue_Author_ID",
            "Status",
            "Issue_Created_Date",
            "Issue_Closed_Date",
            "Issue_Title",
            "Issue_Body",
            "Num_Comments",
            "Num_Review_Comments",
            "Additions",
            "Deletions",
            "Num_Commits",
            "Num_Changed_Files",
        ],
        # ----------------------
    }

    return cols[output_type]


def collect_row_data(issue_num: str, issue_data, separator: str) -> dict:
    """
    Create a dictionary of column names to repo data.

    Note:
        commit info all comes from the last commit right now

    Args:
        issue_num (str): number of current issue
        issue_data (dict): data points of current issue
        separator (str): user-given separator for joining content into strings

    Returns:
        dict: {column name: corresponding repo data point}

    """
    # TODO: all of this should be in a try block?
    issue_col_tbl: dict = {
        "Issue_Author_Login": issue_data["userlogin"],
        "Issue_Author_ID": issue_data["userid"],
        "Issue_Body": issue_data["body"],
        "Issue_Closed_Date": issue_data["closed_at"],
        "Issue_Created_Date": issue_data["created_at"],
        "Issue_Comments": separator.join(
            [comment["body"] for _, comment in issue_data["comments"].items()]
        ),
        "Issue_Num": issue_num,
        "Issue_Title": issue_data["title"],
        "isPR": issue_data["is_pr"],
        "Num_Comments": issue_data["num_comments"],
        "Num_Review_Comments": issue_data["num_review_comments"],
    }

    try:
        issue_col_tbl["PR_Author_Login"] = issue_data["userlogin"]
        issue_col_tbl["PR_Author_Name"] = issue_data["userid"]
        issue_col_tbl["PR_Body"] = issue_data["body"]
        issue_col_tbl["PR_Closed_Date"] = issue_data["closed_at"]
        issue_col_tbl["PR_Title"] = issue_data["title"]
        issue_col_tbl["PR_Comments"] = issue_col_tbl["Issue_Comments"]
        issue_col_tbl["Status"] = issue_data["state"]

    except KeyError:
        issue_col_tbl["PR_Author_Login"] = ""
        issue_col_tbl["PR_Author_Name"] = ""
        issue_col_tbl["PR_Body"] = ""
        issue_col_tbl["PR_Closed_Date"] = ""
        issue_col_tbl["PR_Title"] = ""
        issue_col_tbl["PR_Comments"] = ""
        issue_col_tbl["Status"] = ""

    try:
        _, last_commit = list(issue_data["commits"].items())[-1]
        files = last_commit["files"]

        issue_col_tbl["Additions"] = files["additions"]
        issue_col_tbl["Changes"] = files["changes"]
        issue_col_tbl["Commit_Author_Name"] = last_commit["author_name"]
        issue_col_tbl["Commit_Date"] = last_commit["date"]
        issue_col_tbl["Commit_Message"] = last_commit["message"]
        issue_col_tbl["Deletions"] = files["removals"]
        issue_col_tbl["File_Names"] = files["file_list"]
        issue_col_tbl["Num_Changed_Files"] = str(len(files["file_list"]))
        issue_col_tbl["Num_Commits"] = str(len(issue_data["commits"]))
        issue_col_tbl["Patch_Text"] = files["patch_text"]
        issue_col_tbl["SHA"] = last_commit["sha"]

    except KeyError:
        issue_col_tbl["Additions"] = ""
        issue_col_tbl["Changes"] = ""
        issue_col_tbl["Commit_Author_Name"] = ""
        issue_col_tbl["Commit_Date"] = ""
        issue_col_tbl["Commit_Message"] = ""
        issue_col_tbl["Deletions"] = ""
        issue_col_tbl["File_Names"] = ""
        issue_col_tbl["Num_Changed_Files"] = ""
        issue_col_tbl["Num_Commits"] = ""
        issue_col_tbl["Patch_Text"] = ""
        issue_col_tbl["SHA"] = ""

    return issue_col_tbl


if __name__ == "__main__":
    main()
