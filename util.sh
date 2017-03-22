hdfs dfs -copyFromLocal /opt/hdfs/URLCat/topics.fake.json /topics.fake.json

spark-submit --conf spark.driver.memory=2g --master spark://master:7077 categorize.py 0 0 1 1 0.5

spark-submit server.py 36.14 9.53 48.19 17.13 1 -g