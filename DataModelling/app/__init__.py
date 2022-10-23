from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from app.config.config import config 
class CreateApp():
        
    def create_configuration(self):

        conf = SparkConf().setAppName("Spark Data Modelling") \
                          .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
                          .set('spark.executor.memory', '3g')\
                          .set('spark.driver.memory', '3g')\
                          .set("spark.jars", config['JDBC']) \
                          .set("spark.executor.extraClassPath", config['JDBC']) 

        return conf

    def create_app(self):

        conf = self.create_configuration()

        sc = SparkContext(conf = conf).getOrCreate()
        ssc = StreamingContext(sc, config['BATCH_SCHEDULE'] * 1000) 
        spark = SparkSession(sc)

        return spark







