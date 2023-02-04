import re
import pandas as pd
from contexttimer import Timer

# noinspection PyUnresolvedReferences
from pysam import VariantFile

from lib import columns as c

info_list_regex = re.compile(r'^.*: \'(.+)\'\s*$')
info_list_sep_regex = re.compile(r'[|(]')

gene_id_regex = re.compile(r'^AT[1-5]G\d{5}$')


class VcfImporter(object):
    def __init__(self,
                 data_filter=None,
                 select=None):
        self._filter = data_filter
        self._select = select

    @classmethod
    def can_load(cls, filename):
        vcf_in = None
        # noinspection PyBroadException
        try:
            vcf_in = VariantFile(filename)
            len(vcf_in.header.info)
        except ValueError:
            return False
        except OSError:
            return False
        except Exception:
            return False
        finally:
            if vcf_in:
                # noinspection PyBroadException
                try:
                    vcf_in.close()
                except Exception:
                    pass
        return True

    def import_as_dataframe(self, pool_dir, filename):
        with Timer(factor=1000) as t:
            vcf_in = VariantFile(filename)

            type_info_keys = ['TYPE', 'VARTYPE']
            type_info_key = None
            for _id in type_info_keys:
                if _id in vcf_in.header.info:
                    type_info_key = _id

            if not type_info_key:
                print("warning: cannot find Variant Type (TYPE/VARTYPE) in vcf header, skipping: " + filename)
                return None

            hom_info_keys = ['HOM']
            hom_info_key = None
            for _id in hom_info_keys:
                if _id in vcf_in.header.info:
                    hom_info_key = _id

            if not hom_info_key:
                print("warning: cannot find HOM in vcf header, skipping: " + filename)
                return None

            eff_info_keys = ['EFF', 'ANN']
            eff_info = None
            eff_info_key = None
            for _id in eff_info_keys:
                if _id in vcf_in.header.info:
                    eff_info_key = _id
                    eff_info = vcf_in.header.info.get(eff_info_key)
                    break

            if not eff_info:
                print("warning: cannot find snpEff effects in vcf file, skipping: " + filename)
                return None

            desc = eff_info.description
            desc_match = info_list_regex.match(desc)

            if not desc_match:
                print("warning: unable to load vcf snpEff effect fields, skipping: " + filename)
                return None

            eff_fields = [s.strip(' ][)') for s in info_list_sep_regex.split(desc_match[1])]

            num_eff_field = len(eff_fields)

            if not eff_fields:
                print("warning: unable to parse vcf snpEff effect fields, skipping: " + filename)
                return None

            def split_effects(eff):
                a = [s.strip(' )') or None for s in info_list_sep_regex.split(eff[0])] if eff else []
                missing = num_eff_field - len(a)
                if missing:
                    a.extend([None] * missing)
                return a

            raw_columns = [
                c.chromo,
                c.pos,
                c.ref,
                c.change,
                c.change_type,
                c.hh,
                *eff_fields,
                c.sample
            ]

            def row_gen():
                for rec in vcf_in:
                    chrom = rec.chrom if rec.chrom.startswith('chr') else f'chr{rec.chrom}'
                    pos = rec.pos
                    ref = rec.ref
                    alts = rec.alts
                    num_alts = len(alts)
                    _types = rec.info.get(type_info_key)
                    effects = split_effects(rec.info.get(eff_info_key))
                    hh = 'Hom' if rec.info.get(hom_info_key) else 'Het'
                    for i in range(num_alts):
                        alt = alts[i]
                        _type = _types[i].upper()
                        for sample in rec.samples:
                            yield (
                                chrom,
                                pos,
                                ref,
                                alt,
                                _type,
                                hh,
                                *effects,
                                sample
                            )

            normalized_columns = c.normalize(raw_columns)

            columns = [col for col in normalized_columns if col is not None]

            if len(columns) == len(raw_columns):
                dtype = {}
                defaults = {}

                for col in columns:
                    dtype[col] = c.COLUMNS[col].dtype
                    default = c.COLUMNS[col].na_fill
                    if default is not None:
                        defaults[col] = default

                df: pd.DataFrame = pd.DataFrame(row_gen(), columns=columns).astype(dtype)

                df.fillna(defaults, inplace=True)
            else:
                df = pd.DataFrame(row_gen(), columns=raw_columns)

            vcf_in.close()

            if c.gene_id not in df.columns:

                gene_id_candidates = {}

                if c.gene_name in df.columns:
                    num_gene_ids_in_gene_name_col = df[c.gene_name].str.match(gene_id_regex).sum()

                    gene_id_candidates[c.gene_name] = {
                        'method': 'copied',
                        'matches': num_gene_ids_in_gene_name_col,
                        'column': df[c.gene_name]
                    }

                if c.transcript_id in df.columns:
                    maybe_gene_ids_from_transcript_ids = df[c.transcript_id].str.split('.').str[0]
                    num_gene_ids_in_transcripts = maybe_gene_ids_from_transcript_ids.str.match(gene_id_regex).sum()

                    gene_id_candidates[c.transcript_id] = {
                        'method': 'derived',
                        'matches': num_gene_ids_in_transcripts,
                        'column': maybe_gene_ids_from_transcript_ids
                    }

                if gene_id_candidates:
                    best_col_key = max(gene_id_candidates, key=lambda k: gene_id_candidates[k]['matches'])
                    best = gene_id_candidates[best_col_key]
                    method = best['method']
                    print(f'{c.gene_id} was {method} from {best_col_key} in {filename}')
                    df[c.gene_id] = best['column']

            to_add = {
                c.pool: pool_dir
            }

            df = df.assign(**to_add)

            for col in to_add.keys():
                df[col] = df[col].astype(c.COLUMNS[col].dtype)

            if df.empty:
                print(f'warning: vcf file empty: {filename}')
            else:
                self._filter.apply(df, inplace=True)
                if df.empty:
                    print(f'warning: config filter removes all incoming rows: {filename}')

            if self._select:
                df = df[[*self._select, *to_add.keys(), c.sample]]

            for col in df.columns:
                if df[col].dtype.name == 'category':
                    df[col] = df[col].cat.remove_unused_categories()

            print("vcf import of {} took {}.ms".format(filename, round(t.elapsed, 1)))

            return df
