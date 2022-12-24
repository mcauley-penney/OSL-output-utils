"""Convert JSON files to CSV."""

# TODO:
# 1. Can we create a validation stage that makes sure that certain items exist,
#    ensuring certainty of proper CSV creation
#    - would have to be per row
#
# 2.

import argparse
import csv
from src import file_io_utils as io


def main():
    """
    Driver method for creating CSV files from JSON input.

    Notes:
        1. When the file list of commits has a length
           longer than 1, the list is surrounded by quotes.
           This behavior was in the old extractor too.
    """
    cfg: dict = get_user_cfg()
    input_issues_data: dict = io.read_jsonfile_into_dict(cfg["input_json"])
    columns = get_output_cols(cfg["output_type"])
    rows = []

    rows.append(columns)

    for num, data in input_issues_data.items():
        row_data = collect_row_data(num, data, cfg["separator"])
        ordered_row_data = [row_data[key] for key in columns]
        rows.append(ordered_row_data)

    write_rows(cfg, rows)


def get_user_cfg() -> dict:
    """
    Get path to and read from configuration file.

    :return: dict of configuration values
    :rtype: dict
    """
    cfg_path = get_cli_args()

    return io.read_jsonfile_into_dict(cfg_path)


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


def get_output_cols(output_type: str) -> list:
    """
    Return columns used to organize CSV output.

    Args:
        output_type (str): type of output to return. Can be "pr" or anything
                           else.

    Returns:
        list: list of CSV column names as strings

    """
    cols: dict = {
        # TODO: deprectated?
        #
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
    issue_col_tbl: dict = collect_issue_data(issue_num, issue_data, separator)

    issue_col_tbl = collect_pr_data(issue_col_tbl, issue_data)

    return issue_col_tbl


def collect_issue_data(
    issue_num: str, issue_data: dict, separator: str
) -> dict:
    """
    Collect issue data for the current issue.

        issue_num (str): issue number
        issue_data (dict): issue datapoints, such as author
        separator (str): string used to separate strings composed of other
        strings, such as comments

    Notes:
        We must use the separator parameter to distinguish comments in a
        manner that doesn't interfere with the CSV format, e.g. cannot use
        commas to separate comments if the CSV uses commas as a column
        separator

    Returns:
        dict: data from current issue pertaining to issue endpoints.
    """
    return {
        "Issue_Num": issue_num,
        "Issue_Author_ID": issue_data["userid"],
        "Issue_Author_Login": issue_data["userlogin"],
        "Issue_Body": issue_data["body"],
        "Issue_Closed_Date": issue_data["closed_at"],
        "Issue_Comments": separator.join(
            [comment["body"] for _, comment in issue_data["comments"].items()]
        ),
        "Issue_Created_Date": issue_data["created_at"],
        "Issue_Title": issue_data["title"],
        "isPR": issue_data["is_pr"],
        "Num_Comments": issue_data["num_comments"],
    }


def collect_pr_data(issue_col_tbl: dict, issue_data: dict):
    """
    Update dict of current row data with current issue's PR datapoints.

        issue_col_tbl (dict): current row data
        issue_data (dict): current issue data
    """
    if issue_col_tbl["isPR"] is True:
        # PR data
        issue_col_tbl["Num_Review_Comments"] = issue_data[
            "num_review_comments"
        ]
        issue_col_tbl["PR_Author_Login"] = issue_data["userlogin"]
        issue_col_tbl["PR_Author_Name"] = issue_data["userid"]
        issue_col_tbl["PR_Body"] = issue_data["body"]
        issue_col_tbl["PR_Closed_Date"] = issue_data["closed_at"]
        issue_col_tbl["PR_Title"] = issue_data["title"]
        issue_col_tbl["PR_Comments"] = issue_col_tbl["Issue_Comments"]
        issue_col_tbl["Status"] = issue_data["state"]

        # Commit data
        try:
            commits = list(issue_data["commits"].items())

        # put this into an else block?
            if commits:
                last_commit = commits[-1]
                commit_data = last_commit[1]
                files = commit_data["files"]

                issue_col_tbl["Additions"] = files["additions"]
                issue_col_tbl["Changes"] = files["changes"]
                issue_col_tbl["Commit_Author_Name"] = commit_data["author_name"]
                issue_col_tbl["Commit_Date"] = commit_data["date"]
                issue_col_tbl["Commit_Message"] = commit_data["message"]
                issue_col_tbl["Deletions"] = files["removals"]
                issue_col_tbl["File_Names"] = files["file_list"]
                issue_col_tbl["Num_Changed_Files"] = str(len(files["file_list"]))
                issue_col_tbl["Num_Commits"] = str(len(issue_data["commits"]))
                issue_col_tbl["Patch_Text"] = files["patch_text"]
                issue_col_tbl["SHA"] = commit_data["sha"]

            else:
                for key in [
                    "Additions",
                    "Changes",
                    "Commit_Author_Name",
                    "Commit_Date",
                    "Commit_Message",
                    "Deletions",
                    "File_Names",
                    "Num_Changed_Files",
                    "Num_Commits",
                    "Patch_Text",
                    "SHA",
                ]:
                    issue_col_tbl[key] = ""

        except KeyError:
            for key in [
                "Additions",
                "Changes",
                "Commit_Author_Name",
                "Commit_Date",
                "Commit_Message",
                "Deletions",
                "File_Names",
                "Num_Changed_Files",
                "Num_Commits",
                "Patch_Text",
                "SHA",
            ]:
                issue_col_tbl[key] = ""

    else:
        for key in [
            "Num_Review_Comments",
            "PR_Author_Login",
            "PR_Author_Name",
            "PR_Body",
            "PR_Closed_Date",
            "PR_Title",
            "PR_Comments",
            "Status",
            "Additions",
            "Changes",
            "Commit_Author_Name",
            "Commit_Date",
            "Commit_Message",
            "Deletions",
            "File_Names",
            "Num_Changed_Files",
            "Num_Commits",
            "Patch_Text",
            "SHA",
        ]:
            issue_col_tbl[key] = ""

    return issue_col_tbl


def write_rows(cfg: dict, out_rows: list) -> None:
    """
    Write gathered rows to the output CSV.

        cfg (dict): user configuration
        out_rows (list): list of lists of row data to write

        Returns: None
    """
    with open(cfg["output_csv"], "w", newline="", encoding="utf-8") as out_csv:

        writer = csv.writer(
            out_csv,
            quoting=csv.QUOTE_MINIMAL,
            delimiter=cfg["delimiter"],
            quotechar='"',
            # escapechar="",
        )

        writer.writerows(out_rows)


if __name__ == "__main__":
    main()
