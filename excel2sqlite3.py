#
# Excel sheetからSqlite3のデータベースにインポートする
#
import pandas as pd
import sqlite3
import re, sys
import logging, argparse
from openpyxl import load_workbook
from contextlib import closing

class xlsx2sqlite3:

    def __init__(self, db, xlsx, encoding, table, sheet):
        self.db  = db
        self.xlsx = xlsx
        self.encoding = encoding
        self.table    = table
        self.con = None
        self.cur = None
        self.firstline = 0
        self.sheet = sheet
        self.items = None
        self.cols  = None
        return

    def openFile(self):
        self.items = pd.read_excel( self.xlsx,
                                        sheet_name=self.sheet,
                                        header=0)
        self.cols = self.items.columns
        return
    
    def openDB(self):
        logging.debug('opening db: %s' % self.db)        
        self.con = sqlite3.connect(self.db, isolation_level='EXCLUSIVE')
        self.cur = self.con.cursor()
        logging.debug('opened db: %s %s %s' % (self.db,self.con,self.cur))
        return

    def close(self):
        self.con.close()
        return
    
    def run(self):
            
        if not self.table:
        # ファイル名からテーブル名を決定 '_' のみのファイル名は不可
            table = re.sub(r'(\w*?)\.csv$',r'\1',self.xlsx)
            table = re.sub(r'\W',r'_',table)
            table = re.sub(r'^_*',r'',table)
            self.table = table

        # テーブルが存在していなければ生成する
        if self.check_if_table_exists(self.table):
            logging.debug('table %s exists' % self.table)

        else:
            logging.debug('table %s does not exist' % self.table)
            sql = 'CREATE TABLE ' + self.table + ' ('
            sql += self.create_schema(self.cols)
            sql += ')'
            logging.debug(sql)
            self.cur.execute(sql)

        self.import_xlsx(self.table)

        return

    def import_xlsx(self, table_name):
        
        ph = "?," * (len(self.items.columns) -1 ) + "?"
        sql = "INSERT OR REPLACE INTO %(table_name)s VALUES(%(ph)s)"

        for index, values in self.items.iterrows():
            
            colvals = []
            for value in values[values.keys()]:
                value = str(value)
                value = re.sub(r'\n'," ", value)
                value = re.sub(r',',";", value)
                colvals.append(value)

            colvals = tuple(colvals)

            logging.debug(colvals)
            logging.debug("\n")
            self.cur.execute(sql % locals(), colvals)

        self.con.commit()
        return
            
    def check_if_table_exists(self,table):

        sql = "select count(*) from sqlite_master "
        sql += "where type = 'table' and name = "
        sql += "'" + table + "';"

        logging.debug('checkig if table: %s exists' % self.table)        
        self.cur.execute(sql)

        for row in self.cur:
            result = int(row[0])

        return result > 0

    def create_schema(self, items):

        cols = ''
        for col in items:
            col = re.sub(r'\n'," ",col)
            cols += '"' + col + '",'
        cols = cols[0:-1]
        
        return cols

    
if __name__ == '__main__':

    LOG_FILENAME = 'logging.out'

    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=LOG_FILENAME,
                    filemode='w')

    parser = argparse.ArgumentParser(description='excel2sqlite3.py')
    argvs  = sys.argv
    argc   = len(argvs)

    parser.add_argument('-xlsx', '--xlsx-file', required=True)
    parser.add_argument('-db', '--db-file', default=':memory:')
    parser.add_argument('-enc', '--encoding', default='cp932')
    parser.add_argument('-table', '--table-name')
    parser.add_argument('-sheet', '--sheet-name', default=0)    
    args = parser.parse_args()

    # コンストラクタ
    importer = xlsx2sqlite3(args.db_file, args.xlsx_file, 
                            args.encoding, args.table_name, args.sheet_name)

    # データベースを開く
    importer.openDB()

    # tableにCSVからデータをインポートする
    importer.openFile()    
    importer.run()

    # データベースを閉じる
    importer.close()

