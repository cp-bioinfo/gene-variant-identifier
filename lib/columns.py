from collections import namedtuple

ColumnMeta = namedtuple('Column', ['title', 'dtype', 'alternates', 'xlsx_format', 'na_fill'])

OBJECT = 'object'
FLOAT64 = 'float64'
UINT64 = 'uint64'
CATEGORY = 'category'

chromo = 'chromo'
pos = 'pos'
ref = 'ref'
change = 'change'
change_type = 'change_type'
hh = 'hh'
qual = 'qual'
cov = 'cov'
warns = 'warns'
errors = 'errors'
gene_id = 'gene_id'
gene_name = 'gene_name'
gene_coding = 'gene_coding'
bio_type = 'bio_type'
transcript_id = 'transcript_id'
exon_id = 'exon_id'
exon_rank = 'exon_rank'
effect = 'effect'
effect_impact = 'effect_impact'
functional_class = 'functional_class'
aa_diff = 'aa_diff'
codon_diff = 'codon_diff'
codon_num = 'codon_num'
codon_deg = 'codon_deg'
cds_size = 'cds_size'
codon_circa = 'codon_circa'
aa_circa = 'aa_circa'
custom_int_id = 'custom_int_id'

pool = 'pool'
sample = 'sample'
background = 'background'
cand_pos = 'cand_pos'
cand_gene = 'cand_gene'
cand_gene_hom_ratio = 'cand_gene_hom_ratio'
flagged_gene = 'flagged_gene'

hom = 'Hom'  # TODO: make expected value configurable

# These are needed to "fillna" - otherwise df.pivot_table drops all rows with any NaN/None in the index.
# Note - this means any rows with missing CATEGORY columns may get dropped :(
OBJECT_DEFAULT = ''
UINT64_DEFAULT = -1
FLOAT64_DEFAULT = -1.0

COLUMNS = {

    # SnpEff TXT Format
    chromo: ColumnMeta('Chromo', CATEGORY, None, None, None),
    pos: ColumnMeta('Position', UINT64, None, None, UINT64_DEFAULT),
    ref: ColumnMeta('Reference', CATEGORY, None, None, None),
    change: ColumnMeta('Change', CATEGORY, None, None, None),
    change_type: ColumnMeta('Change_type', CATEGORY, None, None, None),
    hh: ColumnMeta('Homozygous', CATEGORY, None, None, None),
    qual: ColumnMeta('Quality', UINT64, None, None, UINT64_DEFAULT),
    cov: ColumnMeta('Coverage', UINT64, None, None, UINT64_DEFAULT),
    warns: ColumnMeta('Warnings', OBJECT, ['WARNINGS'], None, OBJECT_DEFAULT),
    errors: ColumnMeta('Errors', OBJECT, ['ERRORS'], None, OBJECT_DEFAULT),
    gene_id: ColumnMeta('Gene_ID', OBJECT, None, None, OBJECT_DEFAULT),
    gene_name: ColumnMeta('Gene_name', OBJECT, ['Gene_Name'], None, OBJECT_DEFAULT),
    gene_coding: ColumnMeta('Gene_Coding', OBJECT, None, None, OBJECT_DEFAULT),
    bio_type: ColumnMeta('Bio_type', OBJECT, ['Transcript_BioType'], None, OBJECT_DEFAULT),
    transcript_id: ColumnMeta('Transcript_ID', OBJECT, ['Trancript_ID'], None, OBJECT_DEFAULT),  # [sic] snpEff3.3c
    exon_id: ColumnMeta('Exon_ID', OBJECT, ['Exon'], None, OBJECT_DEFAULT),
    exon_rank: ColumnMeta('Exon_Rank', UINT64, None, None, UINT64_DEFAULT),
    effect: ColumnMeta('Effect', OBJECT, None, None, OBJECT_DEFAULT),
    effect_impact: ColumnMeta('Effect_Impact', OBJECT, None, None, OBJECT_DEFAULT),
    functional_class: ColumnMeta('Functional_Class', OBJECT, None, None, OBJECT_DEFAULT),
    aa_diff: ColumnMeta('old_AA/new_AA', OBJECT, ['Amino_Acid_change'], None, OBJECT_DEFAULT),
    codon_diff: ColumnMeta('Old_codon/New_codon', OBJECT, ['Codon_Change'], None, OBJECT_DEFAULT),
    codon_num: ColumnMeta('Codon_Num(CDS)', UINT64, None, None, UINT64_DEFAULT),
    codon_deg: ColumnMeta('Codon_Degeneracy', OBJECT, None, None, OBJECT_DEFAULT),
    cds_size: ColumnMeta('CDS_size', OBJECT, None, None, OBJECT_DEFAULT),
    codon_circa: ColumnMeta('Codons_around', OBJECT, None, None, OBJECT_DEFAULT),
    aa_circa: ColumnMeta('AAs_around', OBJECT, None, None, OBJECT_DEFAULT),
    custom_int_id: ColumnMeta('Custom_interval_ID', OBJECT, None, None, OBJECT_DEFAULT),

    # output
    pool: ColumnMeta('Pool', CATEGORY, None, None, None),
    sample: ColumnMeta('Sample', CATEGORY, None, None, None),
    background: ColumnMeta('Background', UINT64, None, None, UINT64_DEFAULT),
    cand_pos: ColumnMeta('Candidate: Positional', UINT64, None, None, UINT64_DEFAULT),
    cand_gene: ColumnMeta('Candidate: Gene', UINT64, None, None, UINT64_DEFAULT),
    cand_gene_hom_ratio: ColumnMeta('Gene Hit Homozygosity', FLOAT64, None, {'num_format': '0%'}, FLOAT64_DEFAULT),
    flagged_gene: ColumnMeta('Flagged Gene', UINT64, None, None, UINT64_DEFAULT)
}

COLUMN_KEYS = list(COLUMNS.keys())

# Map of raw input columns names to normalized column
LOOKUP = {
    title: col
    for col, meta in COLUMNS.items()
    for title in {col, meta.title, *(meta.alternates or [])}
}


def normalize(raw_columns):
    return [LOOKUP.get(col) for col in raw_columns]


# Outputted Column Names
OUTPUT_NAMES = {col: meta.title for col, meta in COLUMNS.items()}
