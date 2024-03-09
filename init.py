from config import load_config
from datetime import datetime, timedelta
from enum import Enum
import os
from os import getcwd
import pandas as pd
import random
from sqlalchemy import create_engine, types, URL, text
from sqlalchemy.orm import sessionmaker
from prettytable import PrettyTable


# ------------------------------
# Enum Definitions
# ------------------------------
class Operation(Enum):
    add = 'add'
    delete = 'delete'
    update = 'update'
    view = 'view'

class Purpose(Enum):
    audit = 'audit'
    approval = 'approval'
    onboarding = 'onboarding'
    review = 'review'


class Role(Enum):
    auditor = 'auditor'
    loan_manager = 'loan_manager'
    loan_officer = 'loan_officer'


# ------------------------------
# Schema Definitions
# ------------------------------
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
    "time": types.DateTime(),                           # time operation was conducted
    "new_data": types.String(500),                      # the data added or updated (not removed or viewed)
    "column_modified": types.String(100)                # column being modified
}

data_schema = {
    "applicant_id": types.BigInteger,                   # unique ID for the applicant
    "annual_income": types.Integer,                     # applicant's income for the year
    "applicant_age": types.Integer,                     # age of the applicant
    "work_experience": types.Integer,                   # years of experience in the work force
    "marital_status": types.String(length=25),          # married or single status for applicant
    "house_ownership": types.String(length=25),         # rented, owned, or norent_noown status of an applicant's house
    "vehicle_ownership": types.String(length=5),        # yes or no for applicant owning a car
    "occupation": types.String(length=40),              # the current occupation of the applicant
    "residence_city": types.String(length=50),          # the current city the applicant lives in
    "residence_state": types.String(length=50),         # the state that the city is in
    "years_in_current_employment": types.Integer,       # number of years of current occupation
    "years_in_current_residence": types.Integer,        # number of years in the current housing
    "loan_default_risk": types.Integer                  # is the applicant at risk of defaulting on the loan
}


employee_schema = {
    "employee_id": types.BigInteger,
    "first_name": types.String(100),
    "last_name":types.String(200),
    "email":types.String(100),
    "phone":types.String(50)
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


# ------------------------------
# Function Definitions
# ------------------------------
def add_access_policy(role, purpose, engine, start_time=None, end_time=None):
    """
    Adds an access policy to the 'privacy-policies' table in the database.

    Parameters:
    - role (Role): The role for which access is being granted (Enum: Role).
    - purpose (Purpose): The purpose for granting access (Enum: Purpose).
    - engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object for executing SQL statements.
    - start_time (datetime, optional): The start time of the access policy. Defaults to the current time if not provided.
    - end_time (datetime, optional): The end time of the access policy. Defaults to 5 minutes from the start time if not provided.

    Returns:
    int: The index of the added access policy in the 'privacy-policies' table.
    """
    if start_time == None:
        start_time = datetime.now()
    if end_time == None:
        end_time = start_time + timedelta(minutes=5)

    policy_data = {
        "entity_role": role.value,
        "purpose": purpose.value,
        "start_time": start_time,
        "end_time": end_time
    }

    with engine.connect() as connection:
        result = connection.execute(text("""
            INSERT INTO "privacy_policies" (entity_role, purpose, start_time, end_time)
            VALUES (:entity_role, :purpose, :start_time, :end_time)
            RETURNING index
        """), policy_data)
        policy_id = result.scalar()
        connection.commit()
        print(f'Added policy {policy_id} {policy_data}')
        return policy_id
    

def create_relationship(table_1, table_2, column_1, column_2, engine, cascade_del=False):
    """
    Create a foreign key relationship between two tables.

    Parameters:
    - table_1 (str): Name of the referenced table.
    - table_2 (str): Name of the referencing table.
    - column_1 (str): Column in the referenced table.
    - column_2 (str): Column in the referencing table.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy database engine.
    - cascade_del (boolean): whether or not to allow cascade deletion

    Returns:
    None
    """
    with engine.connect() as connection:
        connection.execute(text(f'''ALTER TABLE "{table_2}" 
                ADD CONSTRAINT fk_{table_1}_{column_1} 
                FOREIGN KEY ({column_2}) 
                REFERENCES {table_1}({column_1})
                {'ON DELETE CASCADE' if cascade_del else ''};'''))
        connection.commit()


def create_sequence(engine):
    """
    This function creates a sequence named 'counter' if it doesn't already exist in the database.

    Parameters:
    - engine: An SQLAlchemy engine object used to connect to the database.

    Returns:
    None
    """
    with engine.connect() as connection:
        connection.execute(text('CREATE SEQUENCE IF NOT EXISTS counter START WITH 1;'))
        connection.commit()


def create_table(name, schema, engine, p_key='index'):
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
    df.to_sql(name, con=engine, if_exists='replace', index=True, dtype=schema)
    print(f'Created {name} table')
    with engine.connect() as connection:
        result = connection.execute(text(f'SELECT * FROM "{name}"'))
        rows = result.keys()
        print('Column names:', *[r for r in rows], sep='\n\t')
        connection.execute(text(f'ALTER TABLE "{name}" ALTER COLUMN "{p_key}" SET DEFAULT nextval(\'counter\');'))
        connection.execute(text(f'ALTER TABLE "{name}" ALTER COLUMN "{p_key}" SET NOT NULL;'))
        connection.execute(text(f'ALTER TABLE "{name}" ADD PRIMARY KEY ({p_key});'))
        connection.commit()
        print("Primary key: index")
    print()


def delete_row(app_id, engine):
    """
    Delete a row from the 'applicant_details' table based on the provided 'applicant_id'. 
    Cascade delete deletes all references from action-history as well 

    Parameters:
    - app_id (int): The unique identifier of the applicant to be deleted.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        connection.execute(text(f'DELETE FROM applicant_details WHERE applicant_id = {app_id};'))
        connection.commit()


def log_view(policy_id, entity_id, data_id, engine):
    """
    Update action_history to refelect an employee viewing data.

    Parameters:
    - policy_id (int): The unique ID of the policy being followed.
    - entity_id (int): The unique ID of the employee who is viewing data.
    - data_id (int): The unique ID of the data being accessed.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    log_action(policy_id, entity_id, data_id, Operation.view, None, None, engine)

def hard_reset(engine):
    """
    Completely resets the PostgreSQL database by dropping the 'public' schema. It then recreates it.

    Parameters:
    - engine: The SQLAlchemy engine object connected to the PostgreSQL database.

    Returns:
    None
    """
    print("Resetting database")
    with engine.connect() as connection:
        connection.execute(text('DROP SCHEMA public CASCADE;'))
        connection.execute(text('CREATE SCHEMA public;'))
        connection.commit()
    print("Database reset")


def log_action(policy_id, entity_id, data_id, operation, new_data, modified_column, engine):
    """
    Log an action into the action history table.

    Parameters:
    - privacy_id (int): The ID of the policy that is being used.
    - entity_id (int): The ID of the entity performing the action.
    - data_id (int): The ID of the data being acted upon.
    - operation (Operation): The operation being performed (Enum: Operation).
    - new_data (str): The new data added or updated.
    - modified_column (str): The column being modified.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        action_data = {
            "policy_id": policy_id,
            "entity_id": entity_id,
            "data_id": data_id,
            "operation": operation.value,
            "time": datetime.now(),
            "new_data": new_data,
            "modified_column": modified_column
        }

        connection.execute(text("""
            INSERT INTO "action_history" (policy_id, entity_id, data_id, operation, time, new_data, column_modified)
            VALUES (:policy_id, :entity_id, :data_id, :operation, :time, :new_data, :modified_column)
        """), action_data)
        connection.commit()

def populate_data(data_name, engine, number_of_rows=-1):
    """
    Populate the data table.

    Parameters:
    - data_name (str): The name of the data table to be populated.
    - engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.
    - number_of_rows (int): The number of rows to be populated.

    Returns:
    None
    """
    policy_id = add_access_policy(Role.loan_officer, Purpose.onboarding, engine)
    entity_id = select_random_employee("employees.csv")
    cwd = getcwd()
    csv_name = "Applicant-details.csv"
    csv_location = os.path.join(cwd, csv_name)

    csv_data = pd.read_csv(csv_location)
    csv_data = pd.read_csv(csv_location, header = 1)
    csv_data = pd.read_csv(csv_location, skiprows = 1, names = ["applicant_id", "annual_income", "applicant_age", 
                                                                "work_experience", "marital_status", "house_ownership", 
                                                                "vehicle_ownership", "occupation", "residence_city", 
                                                                "residence_state", "years_in_current_employment", 
                                                                "years_in_current_residence", "loan_default_risk"])
    if number_of_rows == -1:
        num_rows = len(csv_data)
    else:
        num_rows = min(number_of_rows, len(csv_data))
    
    selected_data = csv_data.head(num_rows)

    with engine.connect() as connection:
        selected_data.to_sql(data_name, con=connection, if_exists='append', index=False)
        connection.commit()
        for row in selected_data.itertuples(name='Applicant_Data'):
            result = connection.execute(text(f'SELECT index FROM "{data_name}" WHERE applicant_id = {row[1]}'))
            data_id = result.scalar()
            connection.commit()
            operation = Operation.add
            new_data = ','.join(map(str, row[1:]))
            modified_column = 'all_columns'
            log_action(policy_id, entity_id, data_id, operation, new_data, modified_column, engine)
        print(f'Added {num_rows} rows.')


def print_table(table_name, engine, truncate=True):
    """
    Print the entire content of the specified table.

    Parameters:
    - table_name (str): The name of the table to be printed.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        columns_query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
        columns_result = connection.execute(text(columns_query))
        columns_info = {row[0]: row[1] for row in columns_result}

        truncated_columns = {col[:15] if len(col) > 15 and truncate else col: data_type for col, data_type in columns_info.items()}

        select_query = f"SELECT * FROM {table_name};"
        result = connection.execute(text(select_query))

        table = PrettyTable(truncated_columns.keys())

        for row in result:
            formatted_row = []
            for col, value, data_type in zip(truncated_columns.keys(), row, truncated_columns.values()):
                if 'timestamp' in data_type:
                    value = value.strftime('%m-%d-%y %H:%M:%S')
                if isinstance(value, str):
                    value = (value[:17] + '...') if len(value) > 20 and truncate else value

                formatted_row.append(value)
            table.add_row(formatted_row)

        # Print the table
        print(f"Table: {table_name}")
        print(table)
        print('\n')


def remove_column_for_applicant(column_name, applicant_id, engine):
    """
    Remove the specified column for a specific applicant in the 'applicant_details' table
    and update 'action_history' table accordingly.

    Parameters:
    - column_name (str): The name of the column to be removed.
    - applicant_id (int): The ID of the applicant whose column is to be removed.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        # Update applicant_details table to get the old value
        result = connection.execute(text(f'SELECT "{column_name}" FROM applicant_details WHERE applicant_id = {applicant_id}'))
        old_data = result.scalar()

        # Update applicant_details table to set the specified column to None
        query = text(f'UPDATE applicant_details SET "{column_name}" = NULL WHERE applicant_id = {applicant_id}')
        connection.execute(query)
        connection.commit()

        # Retrieve the index based on the applicant_id
        result = connection.execute(text(f'SELECT index FROM applicant_details WHERE applicant_id = {applicant_id}'))
        index = result.scalar()

        # Update action_history table to set column_modified to None for offending rows
        query = text(f"UPDATE action_history SET new_data = REPLACE(new_data, '{old_data},', 'NULL,') WHERE data_id = {index} AND new_data LIKE '%{old_data},%'")
        connection.execute(query)
        connection.commit()


    print(f"Column '{column_name}' removed for applicant_id {applicant_id} in 'applicant_details' table and action history updated.\n")


def select_random_employee(csv_file):
    """
    Selects a random employee ID from the CSV file.

    Parameters:
    - csv_file (str): The path to the CSV file containing employee data.

    Returns:
    int: The employee ID.
    """
    # Read the CSV file
    cwd = getcwd()
    csv_location = os.path.join(cwd, csv_file)

    csv_data = pd.read_csv(csv_location, skiprows=1)

    # Select a random row index
    random_index = random.randint(0, len(csv_data) - 1)

    # Select the random row and retrieve the value from the first column
    random_value = csv_data.iloc[random_index, 0]
    random_value = int(random_value)
    return random_value

def update_data(id, column, value, engine):
    """
    Update a specific column with a new value for a row in the 'applicant_details' table.

    Parameters:
    - id (int): The unique identifier of the applicant.
    - column (str): The name of the column to be updated.
    - value (any): The new value to be set in the specified column.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        query = text(f'''UPDATE applicant_details
                     SET {column} = :new_value
                     WHERE applicant_id = :c_id;
                     ''')
        connection.execute(query, new_value=value, c_id=id)
        connection.commit()
    #TODO add row to action history!


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
 
    # reset the database just in case
    hard_reset(engine)

    #create sequence for unique indexes in tables
    create_sequence(engine)

    #initialize databases and set primary keys
    print("Initializing tables...")
    create_table('applicant_details', data_schema, engine)
    create_table('employees', employee_schema, engine)
    create_table('action_history', action_history_schema, engine)
    create_table('privacy_policies', privacy_policy_schema, engine)
    print("Finished initializing tables!")
    
    # create relationships
    print("Setting up table relationships")
    create_relationship("privacy_policies", "action_history", "index", "policy_id", engine)
    create_relationship("applicant_details", "action_history", "index", "data_id", engine, cascade_del=True)
    print("Finished setting relationships!\n")
    
    #add CSV data to applicant_details table
    print("Populating tables...")
    populate_data('applicant_details', engine, 10)
    print("CSV converted to table!\n")   

    # Try printing entire action history table
    print_table('applicant_details', engine)
    print_table('action_history', engine, False)

    # Try removing a column for the third entry
    remove_column_for_applicant("residence_city", 80185, engine)
    
    # Try printing entire action history table should see None now
    print_table('applicant_details', engine)
    print_table('action_history', engine, False)

    #delete_row(75722, engine)
    #add_access_policy(Role.auditor, Purpose.audit, engine)
    # add_access_policy(Role.auditor, Purpose.audit, engine)
