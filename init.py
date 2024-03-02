from config import load_config
from sqlalchemy import create_engine, types
import pandas as pd

data_schema = {
    "Applicant_ID":                 types.BIGINT,       # unique ID for the applicant
    "Annual_Income":                types.INT,          # applicant's income for the year
    "Applicant_Age":                types.INT,          # age of the applicant
    "Work_Experience":              types.INT,          # years of experience in the work force
    "Marital_Status":               types.VARCHAR(10),  # married or single status for applicant
    "House_Ownership":              types.VARCHAR(10),  # rented, owned, or norent_noown status of an applicant's house
    "Vehicle_Ownership(car)":       types.VARCHAR(5),   # yes or no for applicant owning a car
    "Occupation":                   types.VARCHAR(40),  # the current occupation of the applicant
    "Residence_City":               types.VARCHAR(50),  # the current city the applicant lives in
    "Residence_State":              types.VARCHAR(50),  # the state that the city is in
    "Years_in_Current_Employment":  types.INT,          # number of years of current occupation
    "Years_in_Current_Residence":   types.INT,          # number of years in the current housing
    "Loan_Default_Risk":            types.BOOLEAN       # is the applicant at risk of defaulting on the loan
}

action_history_schema = {
    "ID" :          types.BIGINT,       # unique ID for action
    "Policy_ID":    types.BIGINT,       # links with "ID" from privacy_policy
    "Entity_ID":    types.BIGINT,       # employee ID
    "Data_ID":      types.BIGINT,       # links with "Applicant_ID"
    "Operation":    types.Enum(         # the action done on the table
                        'add', 
                        'delete', 
                        'update',
                        'view'),   
    "Time":         types.BIGINT,       # time operation was conducted
    "New_Data":     types.VARCHAR(50),  # the data added or updated (not removed or viewed)
    "Column":       types.VARCHAR(50)   # column being modified
}

privacy_policy_schema = {
    "ID" :          types.BIGINT,       # unique ID for the policy
    "Entity_Role":  types.Enum(         # role being given access
                        'auditor', 
                        'loan_manager', 
                        'loan_officer'),  
    "Purpose":      types.Enum(         # reason why
                        'audit', 
                        'approval', 
                        'onboarding', 
                        'review'),      
    "Start_Time":   types.DateTime,     # start of effective time
    "End_Time":     types.DateTime      # end of effective time
}



if __name__ == '__main__':
    config = load_config()
    engine = create_engine(f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}')
 
#initialize databases

# set primary keys
    
# create enums