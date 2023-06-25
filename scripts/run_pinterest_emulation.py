import random
from time import sleep
import requests
import sqlalchemy

from lib.utils import read_yaml_creds

random.seed(100)


class AWSDBConnector:

    def __init__(self):
        aws_creds = read_yaml_creds("aws_creds")
        self.HOST = f"{aws_creds['HOST']}" 
        self.USER = f"{aws_creds['USER']}" 
        self.PASSWORD = f"{aws_creds['PASSWORD']}" 
        self.DATABASE = f"{aws_creds['DATABASE']}" 
        self.PORT = aws_creds['PORT']

    def create_db_connector(self):
        url = f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        engine = sqlalchemy.create_engine(url)
        connection = engine.connect()
        return connection


def run_infinite_post_data_loop():
    new_connector = AWSDBConnector()
    connection = new_connector.create_db_connector()
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        selected_row = connection.execute(sqlalchemy.text(
            f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
        for row in selected_row.mappings().all():
            result = dict(row)
            requests.post("http://localhost:8000/pin/", json=result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
