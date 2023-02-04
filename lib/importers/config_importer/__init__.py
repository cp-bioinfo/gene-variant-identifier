import os
import glob
import yaml


class ConfigImporter(object):
    def __init__(self, path=None, filename="gene_compare.yaml"):
        if path is None:
            path = []
        elif not isinstance(path, list) and not isinstance(path, tuple):
            path = [path]

        relative_path = os.path.join(*path, filename)

        config_files = glob.glob(os.path.abspath(relative_path))

        self.config_file = config_files[0] if config_files else None

    def load(self):
        return yaml.safe_load(open(self.config_file).read()) if self.config_file else {}
