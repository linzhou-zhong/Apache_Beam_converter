import os
import fsspec
from fsspec import open_files


class FileSystem(object):

    def __init__(self, protocol):
        self.filesystem = fsspec.filesystem(protocol)

    def get_files(self, path, file_pattern):
        files_path = []
        for file in open_files(os.path.join(path, file_pattern)):
            files_path.append(file.path)
        return files_path
