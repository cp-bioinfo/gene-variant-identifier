#!/usr/bin/env ./venv/bin/python

import sys
from PyQt5.QtWidgets import QFileDialog
from PyQt5 import QtWidgets
from PyQt5.QtCore import QCoreApplication
from contexttimer import Timer

from lib import Compare


def run(pool_root):
    with Timer(factor=1000) as t:
        c = Compare(pool_root)
        outfile = c.apply()
        print("total runtime: {}.ms\n".format(round(t.elapsed, 1)))
        print(outfile)

def get_pool_root():
    app = QtWidgets.QApplication(sys.argv)
    options = QFileDialog.Options()
    qfd = QFileDialog()
    pool_root = qfd.getExistingDirectory(qfd, "Select Pool Directory", options=options)
    qfd.showMinimized()
    qfd.close()
    app.exit()
    QCoreApplication.processEvents()
    return pool_root

if __name__ == '__main__':
    pool_root = get_pool_root()
    if pool_root:
        run(pool_root)


