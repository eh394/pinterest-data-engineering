from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from os.path import expanduser
import os

# home = expanduser("~")
# airflow_dir = os.path.join(home, 'airflow')
# assert os.path.isdir(airflow_dir)

default_args = {
    'owner': 'eh394',
    'depends_on_past': False,
    'email': ['hazla.ewa@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2023, 3, 12, 23, 5, 00),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 3, 13),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id='pinterest_dag',
         default_args=default_args,
         # preset can also be replaced with cron expression '0 0 * * *'; code will execute at midnight
         schedule='@once',
         catchup=False,
         tags=['pinterest']
         ) as dag:

    start_zookeeper_task = BashOperator(
        task_id='start_zookeeper',
        bash_command='cd ~/Desktop/kafka/kafka-3.4.0-src && ./bin/zookeeper-server-start.sh ./config/zookeeper.properties',
        dag=dag)

    start_kafka_task = BashOperator(
        task_id='start_kafka',
        bash_command='cd ~/Desktop/kafka/kafka-3.4.0-src && ./bin/kafka-server-start.sh ./config/server.properties',
        dag=dag)
    
    pinterest_spark_task = BashOperator(
        task_id='run_spark_transformations',
        bash_command='cd ~/Desktop/dev2/pinterest && python -m scripts.run_processing_batch',
        dag=dag)

    close_kafka_task = BashOperator(
        task_id='close_kafka',
        bash_command='cd ~/Desktop/kafka/kafka-3.4.0-src && ./bin/kafka-server-stop.sh ./config/server.properties',
        dag=dag)
    
    close_zookeeper_task = BashOperator(
        task_id='close_zookeeper',
        bash_command='cd ~/Desktop/kafka/kafka-3.4.0-src && ./bin/zookeeper-server-stop.sh ./config/zookeeper.properties',
        dag=dag)




# start_zookeeper_task >> start_kafka_task
# start_kafka_task >> pinterest_spark_task
# pinterest_spark_task >> close_kafka_task
# close_kafka_task >> close_zookeeper_task

# start_zookeeper_task >> start_kafka_task >> pinterest_spark_task >> close_kafka_task >> close_zookeeper_task
start_zookeeper_task >> pinterest_spark_task >> close_kafka_task >> close_zookeeper_task

"""
The code above can be expanded to:
(1) include the airflow file in both project and dag directories, i.e. one is master one gets copied over automatically
(2) add operations that automate the entire process, from data being uploaded into API, through Kafka, s3, spark and back into s3 or equivalent
"""

# Note to connect to the airflow UI run the following:
# airflow webserver --port 8080

if __name__ == "__main__":
    dag.test()
