import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

### For connection #####

user ="root"
password = quote_plus('shrabani123@test')
host = 'localhost'
port = 3306
database ='demo'

conn_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
conn_engine = create_engine (conn_string)

#### Test Connection ####

with conn_engine.connect() as connection:
    print ("Connection is okay.")

#### Load the data ####
def load_data(_):
    df =pd.read_csv(r"D:\data_sources\netflix\netflix_titles.csv")
    return df
print ("Data loaded successfully.")

#### transform the data ####
def transform_data (df):
    df = pd.DataFrame(df)
    df = df.drop_duplicates()
    df['cast'] =df['cast'].fillna('unknown')
    df['director']=df['director'].fillna('unknown')
    return df
print ("Transformation of the data complete.")

#### Load the data to databse ###

def data_to_db(df, netflix):
    data=df.to_sql(name =netflix, con =conn_engine, index = False, if_exists = 'replace')
    return data
print(f"The table {'netflix'} is saved in MYSQL database.")


if __name__ == "__main__":
    df = load_data("Select * from netflix")
    transformed_data = transform_data (df)
    data = data_to_db(transformed_data, 'netflix')

