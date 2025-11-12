import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus


user = 'root'
password = quote_plus('shrabani123@test')
host = 'localhost'
port = 3306

database = 'demo'

conn_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

conn_engine = create_engine(conn_string)

###Check the connection###

with conn_engine.connect() as connection:
    print('Connected successfully.')

### Load the data####
def load_data(_):
    df = pd.read_csv(r"D:\data_sources\netflix\netflix_titles.csv")
    return df
print("Data Loaded successfully.")

def transform_data(df):
    df = pd.DataFrame(df)
    df=df.drop_duplicates()
    df['cast']= df['cast'].fillna('unknown')
    df['director'] =df['director'].fillna('unknown')
    return df

def data_to_db (df, Netflix):
    df.to_sql(name=Netflix, con=conn_engine, index=False, if_exists= 'replace')
print(f'the table{'Netflix'} is stored in the MYSQL database.')

if __name__ == '__main__':
    data = load_data ("Select * from netflix")
    transformed_data=transform_data(data)
    data_to_db(transformed_data, 'Netflix')
    print("ETL process is complete.")
