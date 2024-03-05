from config import load_config
from sqlalchemy import create_engine, types, URL, text
import pandas as pd

data_schema = {
    "applicant_id": types.BigInteger,                   # unique ID for the applicant
    "annual_income": types.Integer,                     # applicant's income for the year
    "applicant_age": types.Integer,                     # age of the applicant
    "work_experience": types.Integer,                   # years of experience in the work force
    "marital_status": types.String(length=10),          # married or single status for applicant
    "house_ownership": types.String(length=10),         # rented, owned, or norent_noown status of an applicant's house
    "vehicle_ownership(car)": types.String(length=5),   # yes or no for applicant owning a car
    "occupation": types.String(length=40),              # the current occupation of the applicant
    "residence_city": types.String(length=50),          # the current city the applicant lives in
    "residence_state": types.String(length=50),         # the state that the city is in
    "years_in_current_employment": types.Integer,       # number of years of current occupation
    "years_in_current_residence": types.Integer,        # number of years in the current housing
    "loan_default_risk": types.Boolean                  # is the applicant at risk of defaulting on the loan
}

action_history_schema = {
    "policy_id": types.BigInteger,                      # links with "ID" from privacy_policy
    "entity_id": types.BigInteger,                      # employee ID
    "data_id": types.BigInteger,                        # links with "Applicant_ID"
    "operation": types.Enum(                            # the action done on the table
                    'add', 
                    'delete', 
                    'update',
                    'view',
                    name="operation_enum"),   
    "time": types.BigInteger,                           # time operation was conducted
    "new_data": types.String(50),                       # the data added or updated (not removed or viewed)
    "column": types.String(50)                          # column being modified
}

privacy_policy_schema = {
    "entity_role": types.Enum(                          # role being given access
                    'auditor', 
                    'loan_manager', 
                    'loan_officer',
                    name='role_enum'),  
    "purpose": types.Enum(                              # reason why
                    'audit', 
                    'approval', 
                    'onboarding', 
                    'review',
                    name='purpose_enum'),      
    "start_time": types.DateTime(),                     # start of effective time
    "end_time": types.DateTime()                        # end of effective time
}

def create_table(name, con, schema, engine, p_key='index'):
    """
    Create a table in a SQL database with the specified name, schema, and primary key.

    Parameters:
    - name (str): The name of the table to be created.
    - con (sqlalchemy.engine.Engine): The SQLAlchemy engine or connection object.
    - schema (dict): A dictionary specifying the schema of the table where keys are column names
                    and values are SQLAlchemy data types.
    - engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object for executing SQL statements.
    - p_key (str): The name of the primary key used

    Returns:
    None
    """
    df = pd.DataFrame(columns=schema.keys())#columns=schema.keys(), dtype=schema)
    #df = pd.DataFrame(columns=schema.keys(), dtype=schema)
    df.to_sql(name, con=con, if_exists='replace', index=True, dtype=schema)
    print(f'Created {name} table');
    with engine.connect() as connection:
        result = connection.execute(text(f'SELECT * FROM "{name}"'))
        rows = result.keys()
        print('Column names:', *[r for r in rows], sep='\n\t')

        connection.execute(text(f'ALTER TABLE "{name}" ADD PRIMARY KEY ({p_key});'))
        connection.commit()
        print("Primary key: index")
    print()

if __name__ == '__main__':
    print("Connecting engine to database")
    config = load_config()
    db_url = URL.create(
        "postgresql",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        database=config["database"]
    )
    engine = create_engine(db_url)
    print("Connection established!")
 
    #initialize databases and set primary keys
    print("Initializing tables...")
    create_table('applicant-details', engine, data_schema, engine)
    create_table('action-history', engine, action_history_schema, engine)
    create_table('privacy-policies', engine, privacy_policy_schema, engine)
    print("Finished initializing tables!")