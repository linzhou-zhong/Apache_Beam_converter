import fnmatch
import os


def get_all_files(dir_path, file_pattern):
    if not dir_path or not file_pattern:
        return []

    files_list = []
    for file in os.listdir(path=dir_path):

        if fnmatch.fnmatch(file, file_pattern):
            files_list.append(os.path.join(dir_path, file))

    return files_list
