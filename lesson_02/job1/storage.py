"""
Layer of persistence. Save content to outer warld here.
"""


import json
import os.path


def save_to_disk(json_content, path):
    """
    this function simply writes json content to the provided path
    it creates intermediate directories if they did not exist and deletes the file at path if it existed before
    """
    dirname = os.path.dirname(path)

    os.makedirs(dirname, exist_ok=True)
    if os.path.exists(path):
        os.remove(path)  # remove file if exists

    with open(path, 'wt+') as f:
        json.dump(json_content, f)
