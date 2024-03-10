import init as db
import random

def random_action(engine):
    #operation = random.choices(list(db.Operation), weights = [0, 1, 3, 2])
    operation = random.choices(list(db.Operation), weights = [0, 0, 1, 0])[0]
    print(operation)
    entity = db.select_random_employee(engine)
    data = db.get_random_account(engine)

    if operation == db.Operation.update:
        column= random.choice(list(db.data_schema.keys())[1:])
        if column == 'annual_income':
            pass
        elif column == 'applicant_age':
            pass
        elif column == 'work_experience':
            pass
        elif column == 'marital_status':
            #new_value= 'married' if data[]
            pass
        elif column == 'house_ownership':
            #new_value= 
            pass
        elif column == 'vehicle_ownership':
            pass
        elif column == 'occupation':
            pass
        elif column == 'residence_city':
            pass
        elif column == 'residence_state':
            pass
        elif column == 'years_in_current_employment':
            pass
        elif column == 'years_in_current_residence':
            pass
        elif column == 'loan_default_risk':
            pass
        else:
            print("Some how you got an invalid column name to generate")
            return
    elif operation == db.Operation.view:
        purpose = random.choice([db.Purpose.audit, db.Purpose.review])
        role = random.choice(list(db.Role))
        policy = db.add_access_policy(role, purpose, engine)
        db.log_view(policy, entity, data[0], engine)
        pass
    elif operation == db.Operation.delete:
        pass
    
    #purpose
    # data id
    # operation
    # new value
    # column name
