from os.path import expanduser
import os

home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
assert os.path.isdir(airflow_dir)