from config import load_config
from datetime import datetime, timedelta
from enum import Enum
import os
from os import getcwd
import pandas as pd
from sqlalchemy import create_engine, types, URL, text
from prettytable import PrettyTable


# ------------------------------
# Enum Definitions
# ------------------------------
class Operation(Enum):
    add = 'add'
    delete = 'soft_delete'   # soft delete
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
    "employee_id": types.BigInteger,                      # employee ID
    "data_id": types.BigInteger,                        # links with "Applicant_ID"
    "operation": types.Enum(                            # the action done on the table
                    *[op.value for op in Operation],
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
    "loan_default_risk": types.Boolean,                  # is the applicant at risk of defaulting on the loan
    "is_deleted": types.Boolean
}

employee_schema = {
    "id": types.BigInteger,
    "first_name": types.String(100),
    "last_name":types.String(200),
    "email":types.String(100),
    "phone":types.String(50)
}

privacy_policy_schema = {
    "entity_role": types.Enum(                          # role being given access
                    *[op.value for op in Role],
                    name='role_enum'),  
    "purpose": types.Enum(                              # reason why
                    *[op.value for op in Purpose],
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
        #print(f'Added policy {policy_id} {policy_data}')
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
        if p_key == 'index':
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


def log_view(policy_id, employee_id, data_id, engine):
    """
    Update action_history to refelect an employee viewing data.

    Parameters:
    - policy_id (int): The unique ID of the policy being followed.
    - employee_id (int): The unique ID of the employee who is viewing data.
    - data_id (int): The unique ID of the data being accessed.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    log_action(policy_id, employee_id, data_id, Operation.view, None, None, engine)

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


def log_action(policy_id, employee_id, data_id, operation, new_data, modified_column, engine):
    """
    Log an action into the action history table.

    Parameters:
    - privacy_id (int): The ID of the policy that is being used.
    - employee_id (int): The ID of the employee performing the action.
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
            "employee_id": employee_id,
            "data_id": data_id,
            "operation": operation.value,
            "time": datetime.now(),
            "new_data": new_data,
            "modified_column": modified_column
        }

        connection.execute(text("""
            INSERT INTO "action_history" (policy_id, employee_id, data_id, operation, time, new_data, column_modified)
            VALUES (:policy_id, :employee_id, :data_id, :operation, :time, :new_data, :modified_column)
        """), action_data)
        connection.commit()

def load_applicants(engine, number_of_rows=-1):
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
    employee_id = select_random_employee(engine)
    cwd = getcwd()
    csv_name = "Applicant-details.csv"
    csv_location = os.path.join(cwd, csv_name)
    csv_data = pd.read_csv(csv_location, skiprows = 1, names = data_schema.keys(), dtype={'loan_default_risk':bool})
    csv_data['is_deleted'] = False
    
    if number_of_rows == -1:
        num_rows = len(csv_data)
    else:
        num_rows = min(number_of_rows, len(csv_data))
    
    selected_data = csv_data.head(num_rows)
    with engine.connect() as connection:
        selected_data.to_sql('applicant_details', con=connection, if_exists='append', index=False)
        connection.commit()
        for row in selected_data.itertuples(name='Applicant_Data'):
            result = connection.execute(text(f'SELECT index FROM "applicant_details" WHERE applicant_id = {row[1]}'))
            data_id = result.scalar()
            connection.commit()
            operation = Operation.add
            new_data = ','.join([f'{key}={value}' for key, value in list(row._asdict().items())[1:]])
            modified_column = 'all_columns'
            log_action(policy_id, employee_id, data_id, operation, new_data, modified_column, engine)
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
    chunksize = 80
    with engine.connect() as connection:
        columns_result = connection.execute(text(f'''SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        ORDER BY ordinal_position;'''))
        columns_info = {row[0]: row[1] for row in columns_result}

        truncated_columns = {f'{col[:4]}_{col.rsplit("_", 1)[1]}'[:15] if len(col) > 15 and truncate else col: data_type for col, data_type in columns_info.items()}
        result = connection.execute(text(f"SELECT * FROM {table_name};"))

        table = PrettyTable(truncated_columns.keys())
        row_count = 0
        for row in result:
            formatted_row = []
            for value, data_type in zip(row, truncated_columns.values()):
                if 'timestamp' in data_type:
                    value = value.strftime('%m-%d-%y %H:%M:%S')
                if data_type == 'character varying' and value != None:
                    if(len(value) > chunksize):
                        chunks = [value[i: i + chunksize] for i in range(0, len(value), chunksize)]
                        value = '\n'.join(chunks)
                if value == None:
                    value = "NULL"
                formatted_row.append(value)
            table.add_row(formatted_row)
            row_count += 1
            if(row_count == 20):
                print(table)
                print('\n')
                row_count = 0
                table.clear_rows()      
        # Print the table
        if row_count > 0:
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

        query = text(f'''UPDATE action_history
            SET new_data = 
                CASE
                    WHEN operation = 'add' 
                    THEN REGEXP_REPLACE(new_data, '({column_name}=)[^,]+(,|$)', '\\1NULL\\2')
                    WHEN operation = 'update' AND column_modified = '{column_name}'
                    THEN NULL
                    ELSE new_data
                END
            WHERE data_id = {index};''')
        connection.execute(query)
        connection.commit()


    print(f"Column '{column_name}' removed for applicant_id {applicant_id} in 'applicant_details' table and action history updated.\n")


def load_employees(engine):
    """
    Loads the employee data into employee table.
    NOTE: This must be done first!
    Parameters:
    - engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.

    Returns:
    None
    """
    cwd = getcwd()
    csv_location = os.path.join(cwd, "employees.csv")
    csv_data = pd.read_csv(csv_location, skiprows=1, names=employee_schema.keys())
    
    with engine.connect() as connection:
        csv_data.to_sql('employees', con=connection, if_exists='append', index=False)
        connection.commit()
    
def select_random_employee(engine):
    """
    Selects a random employee ID from the employee table.

    Paramters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    int: The employee ID.
    """
    with engine.connect() as connection:
        result = connection.execute(text("""Select id
                                         From employees
                                         ORDER BY RANDOM()
                                         LIMIT 1;
                                         """))
        return result.fetchone()[0]

def soft_delete(index, engine):
    """
    Soft delete a record in the 'applicant_details' table. Adds entry to action_history

    Parameters:
    - index (int): The index of the record to be soft-deleted.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    with engine.connect() as connection:
        connection.execute(text(f'UPDATE applicant_details SET is_deleted = true WHERE index = {index};'))
        connection.commit()
    policy_id = add_access_policy(Role.loan_manager, Purpose.approval, engine)
    employee_id = select_random_employee(engine)
    log_action(policy_id, employee_id, index, Operation.delete, None, None, engine)

def get_random_account(engine):
    '''
    Returns the values from a random account in the accounts table

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    tuple: values from the selected row
    '''
    with engine.connect() as connection:
        result = connection.execute(text(f"""Select index,{','.join(list(data_schema.keys()))}
                                         From applicant_details
                                         Where is_deleted = false
                                         ORDER BY RANDOM()
                                         LIMIT 1;
                                         """))
        return result.fetchone()


def update_data(id, column, value, engine, index=-1):
    """
    Update a specific column with a new value for a row in the 'applicant_details' table.

    Parameters:
    - id (int): The unique identifier of the applicant.
    - column (str): The name of the column to be updated.
    - value (any): The new value to be set in the specified column.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - index (int): default -1. option to provide index value to prevent looking it up again.
    Returns:
    None
    """
    with engine.connect() as connection:
        query = text(f'UPDATE applicant_details SET {column} = \'{value}\' WHERE applicant_id = {id};')
        connection.execute(query)
        connection.commit()
        if index < 0:
            data_id = connection.execute(text(f'SELECT index FROM applicant_details WHERE applicant_id = {id}')).scalar()
        else:
            data_id = index
    policy_id = add_access_policy(Role.loan_officer, Purpose.audit, engine)
    employee_id = select_random_employee(engine)
    log_action(policy_id, employee_id, data_id, Operation.update, value, column, engine)

def engine():
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
    return engine

def init(engine, num_applicants=-1):
     # reset the database just in case
    hard_reset(engine)

    #create sequence for unique indexes in tables
    create_sequence(engine)

    #initialize databases and set primary keys
    print("Initializing tables...")
    create_table('applicant_details', data_schema, engine)
    create_table('employees', employee_schema, engine, p_key='id')
    create_table('action_history', action_history_schema, engine)
    create_table('privacy_policies', privacy_policy_schema, engine)
    print("Finished initializing tables!")
    
    # create relationships
    print("Setting up table relationships")
    create_relationship("privacy_policies", "action_history", "index", "policy_id", engine)
    create_relationship("applicant_details", "action_history", "index", "data_id", engine, cascade_del=True)
    create_relationship("employees","action_history", "id", "employee_id", engine)
    print("Finished setting relationships!\n")
    
    #add CSV data to applicant_details table
    print("Populating tables...")
    load_employees(engine)
    load_applicants(engine, num_applicants)
    print("CSV converted to table!\n")   

if __name__ == '__main__':
    
    engine = engine()
 
    init(engine, 10)

    # Try printing entire action history table
    print_table('applicant_details', engine)
    print_table('action_history', engine)

    soft_delete(get_random_account(engine)[0],engine)

    print_table('applicant_details', engine)
    print_table('action_history', engine, False)