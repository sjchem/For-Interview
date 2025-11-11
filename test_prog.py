import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

user ='root'
password=quote_plus('shrabani123@test')
host='localhost'
port=3306
database='demo'

conn_string=f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

conn_engine=create_engine(conn_string)

#Test_connection
with conn_engine.connect() as connection:
    print("Connected Successfully")

def load_data(_):
    df=pd.read_csv(r"D:\data_sources\netflix\netflix_titles.csv")
    return df

def load_to_db(df, Netflix):
    df.to_sql(name='Netflix', con=conn_engine, index=False, if_exists='replace')
    print(f"Data loaded successfully to the table {Netflix}")

if __name__ == '__main__':
    Data = load_data('SELECT * from Netflix')
    load_to_db(Data, 'Netflix')

    print("ETL Process Completed Successfully")
