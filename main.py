import psycopg2
from connect import connect
from config import load_config
import init
from sqlalchemy import create_engine, text
from os import getcwd
import os
import pandas as pd



if __name__ == '__main__':
    config=load_config()
    #conn = connect(config)

    # https://python.plainenglish.io/importing-csv-data-into-postgresql-using-python-aee6b5b11816
    #cur = conn.cursor()
    #conn.set_session(autocommit=True)
    print(config)

    engine = create_engine(f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}')
    #conn.commit()
    #cur.close()
    #conn.close()

    cwd = getcwd()
    csv_name = "Applicant-details.csv"

    # print(f'cwd:{cwd}')
    # add csv location to cwd
    csv_location = os.path.join(cwd, csv_name)

    print(f'csv: {csv_location}')

    print(f"Contents of {csv_name}:")
    dataframe = pd.read_csv(csv_location)
    print(dataframe.head(2)) #Reads first two rows of .csv
    print("\n")

    # import CSV data into PostgreSQL
    dataframe.to_sql("applicant-details", engine, if_exists='replace', index=False)

