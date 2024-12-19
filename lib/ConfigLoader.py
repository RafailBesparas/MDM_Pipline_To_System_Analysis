import configparser
from pyspark import SparkConf

# Read the file sbdl.conf and please everything in a dictionary and return the dictionary conf
# The idea is to read the file once at the start of the application and keep all configurations in memory
# Use them whenever needed
# We dont want to read the file multiple times
def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf

#Simiral to get_config but we read the spar.conf file
# This functions creates a Sparkconf object and pass all configurations in Sparkconf
# We keep all spark conf in an Spark conf object.
def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf

# This help us build a where clause at runtime.
# We will be loading account data from the hive table
# We need flexibility to configure filters and conditions.

def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]