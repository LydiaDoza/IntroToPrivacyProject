import init as db
import random
from faker import Faker

def random_action(engine, acc_data=None,can_delete=True):
    """
    Perform a randomly selected an operation (add, update, view, or delete) on applicant_details

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - acc_data (tuple, optional): Account data to be used in the action. (get this from get_random_account())
    - can_delete (bool, optional): Flag to allow deletion actions. Defaults to True.

    Returns:
    None
    """
    operation = random.choices(list(db.Operation), weights = [0, .1, .5, .4])[0]
    entity = db.select_random_employee(engine)
    data = db.get_random_account(engine) if acc_data == None else acc_data

    if operation == db.Operation.update:
        column= random.choice(list(db.data_schema.keys())[1:-1])
        if column == 'annual_income':
            new_value = random.randint(10000, 10000000)
        elif column == 'applicant_age':
            new_value = random.randint(21, 79)
        elif column == 'work_experience':
            new_value = random.randint(0, 20)
        elif column == 'marital_status':
            new_value= 'married' if data[5] == 'single' else 'single'
        elif column == 'house_ownership':
            new_value= 'owned' if data[6] == 'rented' else 'rented' 
        elif column == 'vehicle_ownership':
            new_value= 'yes' if data[7] == 'no' else 'no'
        elif column == 'occupation':
            fake = Faker()
            new_value = fake.job()
            if len(new_value) > 40:
                x = new_value.split(',')
                if len(x) > 1:
                    new_value = x[0]
                new_value.replace("'", "\\'")
                new_value = new_value[:40]
        elif column == 'residence_city':
            fake = Faker()
            new_value = fake.city()
        elif column == 'residence_state':
            fake = Faker()
            new_value = fake.state()
        elif column == 'years_in_current_employment':
            new_value = random.randint(0, 15)
        elif column == 'years_in_current_residence':
            new_value = random.randint(0, 15)
        elif column == 'loan_default_risk':
            new_value = True if data[13] == False else False
        else:
            print(f"Some how you got an invalid column name to generate {column}")
            return
        db.update_data(data[1], column, new_value, engine, index=data[0])

    elif operation == db.Operation.view:
        purpose = random.choice([db.Purpose.audit, db.Purpose.review])
        role = random.choice(list(db.Role))
        policy = db.add_access_policy(role, purpose, engine)
        db.log_view(policy, entity, data[0], engine)

    elif operation == db.Operation.delete:
        if can_delete == True:
            db.soft_delete(data[0], engine)
        else:
            random_action(engine, acc_data=acc_data, can_delete=False)

def init(engine,num_applicants=-1, history_size=-1):
    db.init(engine, num_applicants=num_applicants)
    if(history_size <= 0):
        return
    hs = int(num_applicants * history_size)
    for _ in range(hs):
        random_action(engine)


engine = db.engine()

init(engine, 10, .5)