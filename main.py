import psycopg2
from connect import connect
from config import load_config

def create_table():
    """Create table in the database"""
    commands = (
    """
    CREATE TABLE applicant-details(
        Applicant_ID                PRIMARY KEY,
        Annual_Income               INTEGER,
        Applicant_Age               INTEGER,
        Work_Experience             INTEGER,
        Marital_Status              VARCHAR(10),
        House_Ownership             VARCHAR(15),
        Vehicle_Ownership(car)      VARCHAR(5),
        Occupation                  VARCHAR(40),
        Residence_City              VARCHAR(50),
        Residence_State             VARCHAR(50),
        Years_in_Current_Employment INTEGER,
        Years_in_Current_Residence  INTEGER,
        Loan_Default_Risk           BOOLEAN          
    )
    """
    )

if __name__ == '__main__':
    config=load_config()
    conn = connect(config)

    # https://python.plainenglish.io/importing-csv-data-into-postgresql-using-python-aee6b5b11816
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # TODO Check if database exists https://stackoverflow.com/questions/18389124/simulate-create-database-if-not-exists-for-postgresql
    cur.execute("CREATE DATABASE privacy")

    conn.commit()
    cur.close()
    conn.close()
