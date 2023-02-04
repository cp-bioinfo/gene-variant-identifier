import re
import datetime
from typing import Dict
from contexttimer import Timer
import pandas as pd
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name

import lib.columns as c

SHEET_NAME_SUBS = {
    '[': '(',
    ']': ')',
    ':': ' - ',
    '*': ' ',
    '?': ' ',
    '/': '_',
    '\\': '_'
}

COLORS = {
    'gray': '#808080',
    'green': '#A9E894',
    'yellow': '#FFFA91'
}

HIGHLIGHT_COLORS = {
    c.background: COLORS['gray'],
    c.cand_pos: COLORS['yellow'],
    c.cand_gene: COLORS['green']
}


class XlsxExporter(object):

    def __init__(self, basename):
        self.basename = basename

    def export(self, dataframes: Dict[str, pd.DataFrame]):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')

        outfile = f'{self.basename}.{now}.xlsx'

        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

        for pool_name, df in dataframes.items():
            with Timer(factor=1000) as t:
                self.process_pool(writer, df, pool_name)
                print("{} export took {}.ms".format(pool_name, round(t.elapsed, 1)))

        writer.save()
        writer.close()

        return outfile

    @staticmethod
    def process_pool(writer, df, sheet_name):

        sheet_name = XlsxExporter.clean_sheet_name(sheet_name)

        df.rename(index=str, columns=c.OUTPUT_NAMES).to_excel(writer, sheet_name, index=False)

        af_row, af_col = df.shape

        workbook: xlsxwriter.Workbook = writer.book

        sheet = workbook.get_worksheet_by_name(sheet_name)

        to_format = {
            cc: c.COLUMNS[cc].xlsx_format
            for cc in df.columns
            if cc in c.COLUMNS and c.COLUMNS[cc].xlsx_format
        }

        for col, xlsx_format in to_format.items():
            XlsxExporter.format_column(workbook, sheet, df, col, xlsx_format)

        for col, bg_color in HIGHLIGHT_COLORS.items():
            XlsxExporter.highlight(workbook, sheet, df, col, bg_color)

        for i in range(0, len(df.columns)):
            sheet.set_column(i, i, 15)

        sheet.freeze_panes(1, 0)

        if af_row > 0 and af_col > 0:
            sheet.autofilter(0, 0, af_row - 1, af_col - 1)

    @staticmethod
    def clean_sheet_name(sheet_name):
        for _out, _in in SHEET_NAME_SUBS.items():
            sheet_name = sheet_name.replace(_out, _in)
        return re.sub('\s+', ' ', sheet_name).strip()

    @staticmethod
    def format_column(workbook, sheet, df, col, xlsx_format):
        column = xl_col_to_name(df.columns.get_loc(col))
        _format = workbook.add_format(xlsx_format)
        sheet.set_column(f'{column}:{column}', None, _format)

    @staticmethod
    def highlight(workbook, sheet, df, col, bg_color):
        af_row, af_col = df.shape
        whole_sheet_end = xl_rowcol_to_cell(af_row, af_col - 1)
        column = df.columns.get_loc(col)
        ref = xl_rowcol_to_cell(1, column, col_abs=True)
        _format = workbook.add_format({'bg_color': bg_color})
        sheet.conditional_format(
            f'A2:{whole_sheet_end}',
            {
                'type': 'formula',
                'criteria': f'={ref}>0',
                'format': _format
            }
        )
