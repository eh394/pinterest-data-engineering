import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy

random.seed(100) # what does this line do? 


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        connection = engine.connect()
        return connection

new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(2)  #random.randrange(0, 2)
        random_row = random.randint(0, 11000)
        connection = new_connector.create_db_connector() # had to add this line
        selected_row = connection.execute(sqlalchemy.text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"))
        for row in selected_row.mappings().all(): # had to modify this line, read up 
            result = dict(row)
            requests.post("http://localhost:8000/pin/", json=result)
        connection.close()


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


