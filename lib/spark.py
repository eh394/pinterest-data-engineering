from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
from lib.utils import read_yaml_creds


class SparkConnector:

    def __init__(self, packages, name):
        os.environ['PYSPARK_SUBMIT_ARGS'] = packages
        self.name = name
        self.conf = SparkConf().setAppName(self.name)

    def set_conf_master(self, master):
        self.conf = self.conf.setMaster(master)
        return self

    def init_context(self):
        self.sc = SparkContext(conf=self.conf)
        return self

    def set_context_log_level(self, log_level):
        self.sc.setLogLevel(log_level)
        return self

    def set_context_hadoop_aws_conf(self, creds_filename):
        self.creds = read_yaml_creds(creds_filename)
        hadoopConf = self.sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', f"{self.creds['accessKeyId']}")
        hadoopConf.set('fs.s3a.secret.key', f"{self.creds['secretAccessKey']}")
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                       'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        return self

    def get_session(self):
        self.session = SparkSession(self.sc)
        return self.session
