import os
import re
import glob
import functools
import pandas as pd
from lib import columns as c

GENE_ID_REGEX = re.compile(r'^AT(?:[1-5]|M|C)G[0-9]{5}(?:\.[0-9]+)?$')  # assuming upper & stripped


class FlaggedGenesImporter(object):
    def __init__(self, path=None, filename=None, details=False):
        self.absolute_path = None

        if filename is None:
            return

        if path is None:
            path = []
        elif not isinstance(path, list) and not isinstance(path, tuple):
            path = [path]

        relative_path = os.path.join(*path, filename)

        matching_files = glob.glob(
            os.path.abspath(
                relative_path
            )
        )

        if matching_files:
            self.absolute_path = matching_files[0]
        else:
            print("warning: unable to locate specified flagged genes file: " + relative_path)

        self.details = details

    def load(self):
        if not self.absolute_path:
            return None

        xls = pd.ExcelFile(self.absolute_path)

        flagged_genes = {
            sheet_name: xls.parse(sheet_name, parse_cols=0).iloc[:, 0].astype(str).str.upper().str.strip()
            for sheet_name in xls.sheet_names
        }

        flagged_genes = [
            sheet[sheet.str.match(GENE_ID_REGEX)]
            .drop_duplicates()
            .sort_values()
            .to_frame(name=c.gene_id)
            .assign(**{sheet_name: True})
            .set_index(c.gene_id)
            for sheet_name, sheet in flagged_genes.items()
        ]

        flagged_genes = functools.reduce(
            lambda a, b: a.join(b, how='outer'), flagged_genes, pd.DataFrame()
        ).fillna(False)

        flagged_genes = flagged_genes.assign(**{c.flagged_gene: True})

        cols = flagged_genes.columns.tolist()
        cols = cols[-1:] + cols[:-1] if self.details else cols[-1:]

        return flagged_genes[cols]

