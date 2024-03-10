import init as db
import random
from faker import Faker
import time

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
            new_value = new_value.replace("'", "")
            if len(new_value) > 40:
                x = new_value.split(',')
                if len(x) > 1:
                    new_value = x[0]
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

def init(engine,num_applicants=-1, history_size=-1, acc=None, delete=True):
    """
    Initialize the database with a specified number of applicants and random actions.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - num_applicants (int): Number of applicants to initialize in the database.
                           Default is -1, which means the initialization won't add new applicants.
    - history_size (float): Size of the action history relative to the number of applicants. Default is -1, which means no action history generation.
    - acc (tuple): Account data to use when generating actions. Default is None.
    - delete (bool): Indicates whether the generated actions can include deletion. Default is True.

    Returns:
    None
    """
    db.init(engine, num_applicants=num_applicants)
    if(history_size > 0):
        hs = int(num_applicants * history_size)
        for _ in range(hs):
            random_action(engine, acc_data=acc, can_delete=delete)

def column_delete_test(engine):
    """
    Test function for column deletion.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    init(engine, num_applicants=1)
    victim = db.get_random_account(engine)
    for _ in range(10):
        random_action(engine, acc_data=victim, can_delete=False)
    for _ in range(2):
        db.update_data(victim[1], 'residence_city', Faker().city(), engine)
    db.print_table('applicant_details', engine)
    db.print_table('action_history', engine)
    db.remove_column_for_applicant('residence_city', victim[1],engine)
    db.print_table('applicant_details', engine)
    db.print_table('action_history', engine)

def row_delete_test(engine):
    """
    Test function for row deletion.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
    None
    """
    init(engine, num_applicants=1)
    victim = db.get_random_account(engine)
    for _ in range(10):
        random_action(engine, acc_data=victim, can_delete=False)
    db.soft_delete(victim[0], engine)
    db.print_table('applicant_details', engine)
    db.print_table('action_history', engine)
    db.delete_row(victim[1],engine)
    db.print_table('applicant_details', engine)
    db.print_table('action_history', engine)

def timed_test(num_app, num_hist, del_type, num_iter, engine, num_del=1, seed=-1):
    """
    Measures the average execution time of deletion operations in the database.

    Parameters:
    - num_app (int): Number of applicants to use in test.
    - num_hist (float): Number of history records relative to number of applicants 
    - del_type (str): Type of deletion operation ('row' for row deletion, 'column' for column deletion).
    - num_iter (int): Number of iterations for the test.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - num_del (int): Number of deletion operations per iteration (default is 1).
    - seed (int): Seed for random number generation (default is -1, ignored if < 1)

    Returns:
    float: Average time taken for the deletion operation across all iterations.
    """
    time_sum = 0
    print(f'{del_type} test [iter={num_iter}, num_app={num_app}, num_hist={num_hist}, num_del={num_del}]')
    for i in range(num_iter):
        print(f'\t{i + 1}: ', end='')
        # test set up
        print('Initializing db...', end='')
        if seed > 0:
            random.seed(seed)
        init(engine, num_app)
        victim = db.get_random_account(engine)
        d = True if del_type == 'row' else False
        n = (num_app * num_hist) - 2 if del_type == 'column' else (num_app * num_hist)
        print('Populating action history...', end='')
        for _ in range(n):
            random_action(engine, can_delete=d)
        if del_type == 'column':
            for _ in range(2):
                db.update_data(victim[1], 'residence_city', Faker().city(), engine)
        elif del_type == 'row':
            db.soft_delete(victim[0], engine)
        # run iteration
        print('Running test...', end='')
        s_time = time.time()
        if del_type == 'column':
            db.remove_column_for_applicant('residence_city', victim[1], engine)
        elif del_type == 'row':
            db.delete_row(victim[1], engine)
        f_time = time.time()
        time_sum += f_time - s_time
        print(f'{round((f_time - s_time) * 1000, 5)} ms')
    avg_time = time_sum / num_iter
    return avg_time

engine = db.engine()

avg = timed_test(100, 1, 'column', 3, engine, seed=12345) * 1000
print("average", round(avg, 3), 'ms')
#column_delete_test(engine)
#row_delete_test(engine)