import os
import re
import pandas as pd

from contexttimer import Timer
import concurrent.futures

from lib import utils, columns as c

EXTENSION = 'snpeff'

EXTENSION_RE = re.compile('^(.*)\.' + EXTENSION + '$', re.I)

COMMENT_START = '# '
HEADER_START = COMMENT_START + c.COLUMNS[c.chromo].title
HEADER_SEP = '\t'


class SnpEffImporter(object):
    def __init__(self,
                 data_filter=None,
                 select=None):
        self._filter = data_filter
        self._select = select

    @classmethod
    def can_load(cls, filename):
        try:
            normalized_columns, *_ = cls.extract_columns(filename)
            if not normalized_columns:
                print("file-type appears to be SnpEff TXT, but none of the expected columns were found.")
                return False
            return True
        except UnicodeDecodeError:
            return False
        except StopIteration:
            return False

    def import_as_dataframe(self, pool_dir, filename):
        with Timer(factor=1000) as t:
            sample_name = self.extract_sample_name(filename)

            try:
                df = self.read_snp_txt(filename)
            except StopIteration:
                print("warning: cannot find header in SnpEff TXT file, skipping: " + filename)
                return None

            to_add = {
                c.pool: pool_dir,
                c.sample: sample_name
            }

            df = df.assign(**to_add)

            for col in to_add.keys():
                df[col] = df[col].astype(c.COLUMNS[col].dtype)

            self._filter.apply(df, inplace=True)

            if self._select:
                df = df[[*self._select, *to_add.keys()]]

            for col in df.columns:
                if df[col].dtype.name == 'category':
                    df[col] = df[col].cat.remove_unused_categories()

            df.reset_index(drop=True, inplace=True)

            print("snpeff import of {} took {}.ms".format(filename, round(t.elapsed, 1)))
        return df

    def read_snp_txt(self, filename):
        columns, use_columns, dtypes = self.extract_columns(filename)

        kwargs = {
            'comment': '#',
            'header': None,
            'names': columns,
            'dtype': dtypes,
            'usecols': use_columns,
            'sep': '\t',
            'keep_default_na': False
        }

        return pd.read_csv(filename, **kwargs)

    @staticmethod
    def extract_columns(filename):
        def _get_columns_line(s):
            return s.startswith(HEADER_START)

        with open(filename, 'r') as file:
            try:
                raw_column_str = next(filter(_get_columns_line, file))
            except StopIteration:
                raise StopIteration("Unable to parse snpEff header")

            raw_columns = raw_column_str[len(COMMENT_START):].strip().split(HEADER_SEP)

            normalized_columns = c.normalize(raw_columns)

            use_columns = [col for col in normalized_columns if col is not None]

            dtypes = {col: c.COLUMNS[col].dtype for col in use_columns}

            return normalized_columns, use_columns, dtypes

    @staticmethod
    def extract_sample_name(filename):
        return os.path.splitext(os.path.basename(filename))[0]
