# IntroToPrivacyProject

This is a repo for the CS510 class Introduction to Privacy Aware Computing final project. It focuses on testing the performance of different deletion implementations in a PostgreSQL database that follow different levels of privacy policy compliance

## Setup

First make a python virtual environment from within the project directory.

### Python virtual environment

```cmd
python -m venv venv
```

Activate the virtual environment.

Windows:

```cmd
.\venv\scripts\activate
```

Unix:

```cmd
source ./venv/vin/activate
```

### postgres database creation

After installing postgres from [the postgres website](https://www.postgresql.org/download/), start the psql tool

```cmd
psql -U postgres
```

and then create the database for the project

```psql
CREATE DATABASE privacy_policy;
```

now you may exit the psql

```psql
exit
```

