import init as db
import random
import sys
from faker import Faker
import time
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from sqlalchemy import text

def random_action(engine, blacklist=None, acc_data=None, can_delete=True):
    """
    Perform a randomly selected operation (add, update, view, or delete) on applicant_details

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - acc_data (tuple, optional): Account data to be used in the action. (get this from get_random_account())
    - can_delete (bool, optional): Flag to allow deletion actions. Defaults to True.

    Returns:
    None
    """
    operation = random.choices(list(db.Operation), weights = [0, .1, .5, .4])[0]
    entity = db.select_random_employee(engine)
    data = db.get_random_account(engine, blacklist=blacklist) if acc_data is None else acc_data

    if operation == db.Operation.update:
        column= random.choice(list(db.data_schema.keys())[1:-1])
        new_value = gen_new_value(column, data)
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
            random_action(engine, blacklist=blacklist, acc_data=acc_data, can_delete=False)


def gen_random_action(engine, acc_data=None,can_delete=True):
    """
    Generate the values for a randomly selected an operation (add, update, view, or delete) on applicant_details

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - acc_data (tuple, optional): Account data to be used in the action. (get this from get_random_account())
    - can_delete (bool, optional): Flag to allow deletion actions. Defaults to True.

    Returns:
    dict: Dictionary containing values for the performed action.
    """
    action= {"policy_id": None, 
            "employee_id": None, 
            "data_id": None,
            "operation": None, 
            "time": datetime.now(),
            "new_data": None, 
            "modified_column": None}
    operation = random.choices(list(db.Operation), weights = [0, .1, .5, .4])[0]
    employee = db.select_random_employee(engine)
    data = db.get_random_account(engine) if acc_data == None else acc_data

    action['operation'] = operation.value
    action['employee_id'] = employee
    action['data_id'] = data[0]

    if operation == db.Operation.update:
        column= random.choice(list(db.data_schema.keys())[1:-1])
        action['modified_column'] = column
        action['new_data'] = gen_new_value(column, data)
        p_id = db.add_access_policy(db.Role.loan_officer, db.Purpose.audit, engine)
    elif operation == db.Operation.view:
        purpose = random.choice([db.Purpose.audit, db.Purpose.review])
        role = random.choice(list(db.Role))
        p_id = db.add_access_policy(role, purpose, engine)
    elif operation == db.Operation.delete:
        if can_delete:
            p_id = db.add_access_policy(db.Role.loan_manager, db.Purpose.approval, engine)
        else:
            return gen_random_action(engine, acc_data=acc_data, can_delete=can_delete)
    action['policy_id'] = p_id
    return action

def gen_random_actions(engine, num_actions, acc_data=None, can_delete=True):
    """
    Generate a list of randomly selected actions (add, update, view, or delete) on applicant_details.

    Parameters:
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - num_actions (int): The number of random actions to generate.
    - acc_data (tuple, optional): Account data to be used in the actions (get this from get_random_account()).
    - can_delete (bool, optional): Flag to allow soft deletion actions. Defaults to True.

    Returns:
    list: List of dictionaries containing information about the generated actions.
    """
    actions = []
    for i in range(num_actions):
        actions.append(gen_random_action(engine, acc_data= acc_data, can_delete=can_delete))
    return actions

def random_actions(engine, num_actions, blacklist=None, can_delete=True):
    for _ in range(num_actions):
        random_action(engine, blacklist=blacklist, can_delete=can_delete)

def gen_new_value(column, data):
    if column == 'annual_income':
        return random.randint(10000, 10000000)
    elif column == 'applicant_age':
        return random.randint(21, 79)
    elif column == 'work_experience':
        return random.randint(0, 20)
    elif column == 'marital_status':
        return 'married' if data[5] == 'single' else 'single'
    elif column == 'house_ownership':
        return 'owned' if data[6] == 'rented' else 'rented'
    elif column == 'vehicle_ownership':
        return 'yes' if data[7] == 'no' else 'no'
    elif column == 'occupation':
        fake = Faker()
        new_value = fake.job()
        new_value = new_value.replace("'", "")
        if len(new_value) > 40:
            x = new_value.split(',')
            if len(x) > 1:
                new_value = x[0]
            new_value = new_value[:40]
        return new_value
    elif column == 'residence_city':
        fake = Faker()
        return fake.city()
    elif column == 'residence_state':
        fake = Faker()
        return fake.state()
    elif column == 'years_in_current_employment':
        return random.randint(0, 15)
    elif column == 'years_in_current_residence':
        return random.randint(0, 15)
    elif column == 'loan_default_risk':
        return True if data[13] == False else False
    else:
        print(f"Somehow you got an invalid column name to generate {column}")
        return None


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
    db.remove_column_for_applicant('residence_city', victim[0],engine, vacuum=True)
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

def get_ids(engine):
    with engine.connect() as connection:
        result = connection.execute(text('SELECT index FROM applicant_details;'))
        indexs = [row[0] for row in result]
    return indexs

def timed_test(num_app, num_hist, num_iter, vacuum, engine, num_del=1, seed=-1):
    """
    Measures the average execution time of a single column deletion operation in the database.

    Parameters:
    - num_app (int): Number of applicants to use in test.
    - num_hist (float): Number of history records relative to number of applicants 
    - num_iter (int): Number of iterations for the test.
    - vacuum (bool): Execute VACUUM FULL after deletion.
    - engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.
    - num_del (int): Number of deletion operations per iteration (default is 1).
    - seed (int): Seed for random number generation (default is -1, ignored if < 1)

    Returns:
    float: Average time taken for the deletion operation across all iterations.
    """
    # test set up
    if seed > 0:
        random.seed(seed)
    time_sum = 0
    n = int(num_app * num_hist) - (2 * num_iter)
    print(f'Column test [iter={num_iter}, num_app={num_app}, num_hist={num_hist}, num_del={num_del}]')

    print('Initializing db...', end='')
    init(engine, num_app)
    
    print('Populating action history...')
    selected_ids = random.choices(get_ids(engine), k=num_iter)
    # add update calls for the test to have to overwrite
    for i in selected_ids:
        for _ in range(2):
            db.update_data(None, 'residence_city', Faker().city(),engine, index=i)
    random_actions(engine, n, selected_ids)

    for i in range(num_iter):
        print(f'\t{i + 1}: ', end='')
        
        victim = selected_ids[i]
        
        print('Running test...', end='')
        s_time = time.time()
        db.remove_column_for_applicant('residence_city', victim, engine, vacuum=vacuum)
        f_time = time.time()
        time_sum += f_time - s_time
        print(f'{round((f_time - s_time) * 1000, 5)} ms')
    avg_time = time_sum / num_iter
    return avg_time

def batch_timed_test(num_iter, is_sequential, selected_ids):
    time_sum = 0
    
    for i in range(num_iter):
        print(f'\t{i + 1}: ', end='')
        print('Running test...', end='')
        s_time = time.time()
        db.column_batch_delete('residence_city', selected_ids, is_sequential, engine)    
        f_time = time.time()
        time_sum += f_time - s_time
        print(f'{round((f_time - s_time), 5)} s')
    avg_time = time_sum / num_iter
    return avg_time

def evaluate(total_app, hist_size, engine, num_steps = 4, num_iter=5, seed=-1):
    step_size = total_app // num_steps
    test_sizes = range(step_size, total_app + 1, step_size)
    avg_times = []

    for size in test_sizes:
        avg_time = timed_test(size, hist_size, num_iter, True, engine, seed=seed)
        avg_time *= 1000
        print(f'\tAverage: {round(avg_time, 3)}ms')
        avg_times.append(avg_time)

    # Plotting the results
    plt.plot(test_sizes, avg_times, marker='o')
    plt.title(f'Performance relative to data size ({int(100 + hist_size * 100)}% History Size)')
    plt.xlabel('Number of Applicants')
    plt.ylabel('Average Time (ms)')
    plt.grid(True)
    plt.show()

def evaluate_hist(total_app, hist_inc, engine, num_steps=4, num_iter=5, seed=-1):
    avg_times = []
    step = int(total_app * hist_inc)
    step_sizes = range(total_app + step, total_app + (step * num_steps) + 1, step)
    for n in range(1,num_steps + 1):
        avg_time = timed_test(total_app, hist_inc * n, num_iter, True, engine, seed=seed)
        avg_time *= 1000
        print(f'\tAverage: {round(avg_time, 3)}ms')
        avg_times.append(avg_time)
    
    plt.plot(step_sizes, avg_times, marker='o')
    plt.title(f'Performance Relative to Action History size({total_app} applicants)')
    plt.xlabel('History Size:')
    plt.ylabel('Average Time (ms)')
    plt.grid(True)
    plt.show()

def batch_evaluate(total_app, hist_size, num_deletes, is_sequential, engine, num_steps = 4, num_iter=5, init_db=False):
    step_size = num_deletes // num_steps
    test_sizes = range(step_size, num_deletes + 1, step_size)
    avg_times = []
    
    if init_db:
        # test set up
        if seed > 0:
            random.seed(seed)
        
        n = int(total_app * hist_size) - (2 * num_iter)
        print(f"Batch {' (Sequential)' if is_sequential else ''} test num_app={total_app}, num_hist={hist_size * total_app}, num_del={num_deletes} num_iter={num_iter}  num_steps={num_steps}")

        print('Initializing db...', end='')
        init(engine, total_app)
        
        print('Populating action history...')
        selected_ids = random.choices(get_ids(engine), k=num_deletes)
        for i in selected_ids:
            for _ in range(2):
                db.update_data(None, 'residence_city', Faker().city(),engine, index=i)
        random_actions(engine, n, selected_ids)
    else:
        selected_ids = random.choices(get_ids(engine), k=num_deletes)
            
    i = 1
    for size in test_sizes:
        print(f'[iter={str(i) + "/"+ str(num_steps)} num_delete={size}]')
        avg_time = batch_timed_test(num_iter, is_sequential, selected_ids[:size])
        #avg_time *= 1000
        print(f'\tAverage: {round(avg_time, 3)}s')
        avg_times.append(avg_time)
        i += 1
    
       # Plotting the results
    plt.plot(test_sizes, avg_times, marker='o')
    s = ' (sequential)' if is_sequential else ''
    plt.title(f'Batch{s} Deletion Performance')
    plt.xlabel('Number of Deletions')
    plt.ylabel('Average Time (s)')
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    engine = db.engine()
    db.hard_reset(engine)
    
    column_delete_test(engine)
    row_delete_test(engine)
    seed = 344323422
    # data size performance test
    evaluate(100000, .5, engine, num_iter=10, num_steps=4, seed=seed)
    # history performance test
    evaluate_hist(2000, 1, engine, num_steps=8, num_iter=10, seed=seed)
    # batch test
    batch_evaluate(100000, 1, 75000, False, engine, num_steps=5, num_iter=10, init_db=True)
    # sequential batch test
    batch_evaluate(100000, 1, 75000, True, engine, num_steps=5, num_iter=10, init_db=False)