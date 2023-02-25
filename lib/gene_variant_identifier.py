import os
import glob
from natsort import natsorted
from contexttimer import Timer
import concurrent.futures

from .filters import BooleanFilterTree
from .exporters import XlsxExporter
from .importers import ConfigImporter, FlaggedGenesImporter, SnpEffImporter, VcfImporter

from . import utils
from . import columns as c

CONFIG_FIELDS = {
    'FILTERS': 'Filters',
    'SELECT_COLS': 'Select Columns',
    'FLAGGED_GENES_PATH': 'Flagged Genes Path',
    'FLAGGED_GENE_DETAILS': 'Flagged Genes Details'
}


class GeneVariantIdentifier(object):
    def __init__(self, pool_root):
        if not pool_root or not os.path.isdir(pool_root):
            raise RuntimeError("Please call using a directory, not a specific file.")

        self.pool_root = pool_root

        pool_dirs = natsorted(next(os.walk(pool_root))[1])

        if not pool_dirs:
            raise RuntimeError(f"No pool sub-directories found in {pool_root}.")

        self.pool_dirs = pool_dirs

        self.config = ConfigImporter(pool_root).load()

        self._select = self.config.get(CONFIG_FIELDS['SELECT_COLS'])

        for col in self._select:
            if col not in c.COLUMN_KEYS:
                raise RuntimeError(
                    f"Selected column '{col}' not a recognized column key.\n"
                    f"Must be one of {c.COLUMN_KEYS}"
                )

        self.data_filter = BooleanFilterTree(self.config.get(CONFIG_FIELDS['FILTERS']))

        self.loaders = [
            VcfImporter(
                data_filter=self.data_filter,
                select=self._select
            ),
            SnpEffImporter(
                data_filter=self.data_filter,
                select=self._select
            )
        ]

        self.loader_map = {}

        for pool_dir in pool_dirs:
            for path, loader in self._loader_map(pool_dir).items():
                self.loader_map[path] = loader

        flagged_genes_path = self.config.get(CONFIG_FIELDS['FLAGGED_GENES_PATH'])
        flagged_genes_details = self.config.get(CONFIG_FIELDS['FLAGGED_GENE_DETAILS'])

        self.flagged_genes_loader = None

        if flagged_genes_path:
            self.flagged_genes_loader = FlaggedGenesImporter(
                path=pool_root,
                filename=flagged_genes_path,
                details=flagged_genes_details
            )

        self.exporter = XlsxExporter(basename=pool_root)

    def _loader_map(self, pool_dir):
        filenames = glob.glob(
            os.path.abspath(
                os.path.join(
                    self.pool_root,
                    pool_dir,
                    '*'
                )
            )
        )

        filenames = filter(os.path.isfile, filenames)

        loader_map = {}
        for filename in filenames:
            loader = self._loader_for(filename)
            if loader:
                loader_map[(pool_dir, filename)] = loader

        return loader_map

    def _loader_for(self, filename):
        for loader in self.loaders:
            if loader.can_load(filename):
                return loader
        print(f'unable to identify file: {filename}')
        return None

    def apply(self):
        df = self.load_dataframes()

        pivots = self.analyse(df)

        with Timer(factor=1000) as t:
            outfile = self.exporter.export(pivots)
            print("xlsx export took {}.ms total".format(round(t.elapsed, 1)))

        return outfile

    def load_dataframes(self):

        df = None

        with Timer(factor=1000) as t:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future_map = {
                    executor.submit(loader.import_as_dataframe, pool_dir, filename):
                        (type(loader).__name__, pool_dir, filename)
                    for (pool_dir, filename), loader in self.loader_map.items()
                }
                for future in concurrent.futures.as_completed(future_map):
                    loader_name, pool_dir, filename = future_map[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise RuntimeError(
                            '%r generated an exception while loading %r/%r: %s' % (loader_name, pool_dir, filename, exc)
                        ) from exc
                    else:
                        if isinstance(df, type(None)):
                            df = result
                        else:
                            df = utils.df_append(df, result, merge_categories=True, ignore_index=True)
            df.reset_index(drop=True, inplace=True)
            print("pool imports took {}.ms total".format(round(t.elapsed, 1)))
        return df

    def analyse(self, full_df):
        with Timer(factor=1000) as t:
            full_df = self.add_flagged_genes(full_df)

            full_df = self.add_background_mutations(full_df)

            full_df = self.add_candidate_pos_mutations(full_df)

            full_df = self.add_candidate_gene_mutations(full_df)

            full_df = self.add_candidate_gene_hh_ratios(full_df)

            print("mutation analysis took {}.ms".format(round(t.elapsed, 1)))

        with Timer(factor=1000) as t:
            dfg = full_df.groupby(c.pool)

            idx = [col for col in full_df.columns if col not in {c.sample, c.pool}]

            dfs = {
                pool: dfg.get_group(pool)
                for pool in natsorted(dfg.groups.keys())
            }

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future_map = {
                    executor.submit(self._pivot, idx, pool, df): pool
                    for pool, df in dfs.items()
                }
                for future in concurrent.futures.as_completed(future_map):
                    pool = future_map[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise RuntimeError(
                            '%r generated an exception: %s' % (pool, exc)
                        ) from exc
                    else:
                        dfs[pool] = result

            print("pool splitting took {}.ms total".format(round(t.elapsed, 1)))

        with Timer(factor=1000) as t:
            dfs[c.COLUMNS[c.background].title] = full_df[full_df[c.background] > 0].sort_values(
                    by=[c.background, c.chromo, c.pos, c.hh],
                    ascending=[0, 1, 1, 0]).reset_index(drop=True)

            dfs[c.COLUMNS[c.cand_pos].title] = full_df[
                (full_df[c.cand_pos] > 0) & (full_df[c.background] == 0)].sort_values(
                by=[c.cand_pos, c.chromo, c.pos, c.hh],
                ascending=[0, 1, 1, 0]).reset_index(drop=True)

            dfs[c.COLUMNS[c.cand_gene].title] = full_df[
                (full_df[c.cand_gene] > 0) & (full_df[c.background] == 0)].sort_values(
                by=[c.cand_gene, c.chromo, c.pos, c.hh],
                ascending=[0, 1, 1, 0]).reset_index(drop=True)

            if self.flagged_genes_loader and c.flagged_gene in full_df.columns:
                dfs[c.COLUMNS[c.flagged_gene].title] = full_df[
                    (full_df[c.flagged_gene] > 0) & (full_df[c.background] == 0)].sort_values(
                    by=[c.chromo, c.pos, c.hh],
                    ascending=[1, 1, 0]).reset_index(drop=True)

            print("summary results took {}.ms".format(round(t.elapsed, 1)))

        return dfs

    def add_flagged_genes(self, df):
        if not self.flagged_genes_loader:
            return df

        flagged_genes = self.flagged_genes_loader.load()

        if (isinstance(flagged_genes, type(None))) or not len(flagged_genes):
            return df

        df = df.merge(flagged_genes, left_on=c.gene_id, right_index=True, how='left')
        fill_nas = {col: False for col in flagged_genes.columns}
        return df.fillna(value=fill_nas)

    @staticmethod
    def add_background_mutations(df):
        return GeneVariantIdentifier._add_hit_column(df, c.background, [c.chromo, c.pos], c.pool)

    @staticmethod
    def add_candidate_pos_mutations(df):
        return GeneVariantIdentifier._add_hit_column(df, c.cand_pos, [c.pool, c.chromo, c.pos], c.sample)

    @staticmethod
    def add_candidate_gene_mutations(df):
        return GeneVariantIdentifier._add_hit_column(df, c.cand_gene, [c.gene_id], c.sample)

    @staticmethod
    def add_candidate_gene_hh_ratios(df):
        if df.empty:
            return df

        hh_values = list(df.dtypes[c.hh].categories)  # ['Het', 'Hom']

        if c.hom not in hh_values:
            print(f'{c.hom} not found in {c.hh} values: {str(hh_values)}')
            return df

        df_hh = df.pivot_table(index=[c.gene_id], columns=[c.hh], aggfunc={c.sample: 'count'}, fill_value=0)

        # percentage of total that are Homo
        df_hh = (df_hh[(c.sample, c.hom)] / sum(df_hh[(c.sample, h)] for h in hh_values))

        df_hh = df_hh.to_frame(name=c.cand_gene_hom_ratio)

        return df.merge(
            df_hh,
            left_on=[c.gene_id],
            right_on=[c.gene_id],
            how='inner'
        )

    @staticmethod
    def _add_hit_column(df, title, intersect, count_by, agg='nunique'):
        return df.merge(
            (df.groupby(intersect)[count_by].agg(agg) - 1).to_frame(name=title),
            left_on=intersect,
            right_on=intersect,
            how='inner'
        )

    @staticmethod
    def _pivot(idx, pool, df):
        with Timer(factor=1000) as t:
            def agg(x):
                return len(x)

            df1 = df.pivot_table(
                index=idx,
                columns=[c.sample],
                aggfunc=agg,
                fill_value=0
            )

            df2 = df1.sort_values(
                by=[c.chromo, c.pos, c.hh],
                ascending=[1, 1, 0])

            df = utils.reset_categorical_index(df2)

            print("splitting {} took {}.ms".format(pool, round(t.elapsed, 1)))

        return df
