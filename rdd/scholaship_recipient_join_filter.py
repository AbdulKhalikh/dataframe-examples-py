from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
        # org.apache.hadoop:hadoop-aws:2.7.4 => used to read and write data from aws s3
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("RDD examples") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR') # used to see error logs not all logs like warn etc..

    current_dir = os.path.abspath(os.path.dirname(__file__)) # current file(scholarship_re_join_filter) path stored in current_dir variable
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml") # application yml path
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets") # secrets file path programmatically

    conf = open(app_config_path) # conf is file object point to yml path
    app_conf = yaml.load(conf, Loader=yaml.FullLoader) # app_conf contains data of yml file in key value pairs
    secret = open(app_secrets_path) # secret is file object point to secrets path
    app_secret = yaml.load(secret, Loader=yaml.FullLoader) # app_secret contains data of secrets file in key value pairs

    # Setup spark to use s3
    # setting and access the  access key and secrets key (test.pem), app_secret is a key value pair , app_secret["s3_conf"]["access_key"] - used to read data
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    demographics_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")
    finances_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7]))))

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), (strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4]))))

    print('Participants belongs to \'Switzerland\', having debts and financial dependents,')
    join_pair_rdd = demographics_pair_rdd \
        .join(finances_pair_rdd) \
        .filter(lambda rec: (rec[1][0][2] == "Switzerland") and (rec[1][1][0] == 1) and (rec[1][1][1] == 1)) \

    join_pair_rdd.foreach(print)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/scholaship_recipient_join_filter.py
