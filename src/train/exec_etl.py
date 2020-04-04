import pandas as pd
import os
from olistlib.db import utils
import argparse

import datetime
from dateutil.relativedelta import relativedelta

parser = argparse.ArgumentParser()
parser.add_argument("--date_init", '-i', help='Data referencia para incio da ABT')
parser.add_argument("--date_end", '-e', help='Data referencia para término da ABT')
parser.add_argument("--save_db", '-d', help='Deseja salvar no banco de dados?', action='store_true')
parser.add_argument("--save_file", '-f', help='Deseja salvar em um arquivo?', action='store_true')
args = parser.parse_args()

TRAIN_DIR = os.path.dirname( os.path.abspath( __file__ ) )
SRC_DIR = os.path.dirname( TRAIN_DIR )
BASE_DIR = os.path.dirname( SRC_DIR )
DATA_DIR = os.path.join( BASE_DIR, 'data')

OUT_BASE_DIR = os.path.dirname( BASE_DIR )
OUT_DATA_DIR = os.path.join( OUT_BASE_DIR, "upload_olist", 'data' )
DB_PATH = os.path.join( OUT_DATA_DIR, "olist.db" )

# Cria lista de datas...
date = datetime.datetime.strptime( args.date_init, "%Y-%m-%d" )
date_end = datetime.datetime.strptime( args.date_end, "%Y-%m-%d" )
dates = []
while  date <= date_end:
    dates.append( date.strftime( "%Y-%m-%d" ) )
    date += relativedelta( months = 1 )

print("\n Abrindo conexão com banco de dados...")
con = utils.connect_db( 'sqlite', path=DB_PATH )
print(" Ok.")

print("\n Executando a extração dos dados...")
# Query de features base
query_etl_base = utils.import_query( os.path.join( TRAIN_DIR, 'etl.sql' ) )

# Query para abt_base
query_abt_base = utils.import_query( os.path.join( TRAIN_DIR, 'make_abt.sql' ) )

dfs = []
for d in dates:
    query_etl = query_etl_base.format( date=d, stage="TRAIN" )
    query_abt = query_abt_base.format( date=d )
    utils.execute_many_sql( query_etl, con )
    dfs.append( pd.read_sql_query( query_abt, con ) )

df = pd.concat( dfs, axis=0, ignore_index=True )
print(" Ok.")

if args.save_db:
    print("\n Salvando dados em Banco de dados...")
    table_name = 'tb_abt_{date_init}_{date_end}'.format( date_init = args.date_init.replace( "-", "" ) ,
                                                         date_end = args.date_end.replace( "-", "" ) )
    df.to_sql( table_name, con, index=False, if_exists='replace' )
    print(" Ok.")

if args.save_file:
    print("\n Salvando dados em arquivo...")
    table_name = 'tb_abt_{date_init}_{date_end}.csv'.format( date_init = args.date_init.replace( "-", "" ) ,
                                                             date_end = args.date_end.replace( "-", "" ) )
    df.to_csv( os.path.join( DATA_DIR, table_name), index=False)
    print(" Ok.")