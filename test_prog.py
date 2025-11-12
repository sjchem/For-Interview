import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import streamlit as st

st.title("🎬 Netflix ETL & Query App")

# === Database Connection ===
user = 'root'
password = quote_plus('shrabani123@test')
host = 'localhost'
port = 3306
database = 'demo'

conn_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
conn_engine = create_engine(conn_string)

# === ETL Button ===
if st.button("Run ETL Process"):
### Load the data ###
    df = pd.read_csv(r"D:\data_sources\netflix\netflix_titles.csv")
    st.success("Loading process is complete.")
### Transform the data ###
    df=pd.DataFrame(df)
    df = df.drop_duplicates()
    df['cast'] = df['cast'].fillna ('unknown')
    df['director'] = df['director'].fillna ('unknown')
    st.success("Data is transformed in correct format.")

### Load the table to MYSQL database ###
    df.to_sql(name='netflix', con=conn_engine, index=False, if_exists='replace')
    st.success("✅ ETL Completed! Table 'Netflix' created in MySQL.")


# === SQL Query Section ===
query = st.text_input("Enter SQL Query:", "SELECT * FROM netflix")

if st.button("Run Query"):
    
    with conn_engine.connect() as conn:
        df_query = pd.read_sql(text(query), conn)
        st.dataframe(df_query)




