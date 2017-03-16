hdfs dfs -copyFromLocal /opt/hdfs/URLCat/topics.fake.json /topics.fake.json

spark-submit --conf spark.driver.memory=2g --master spark://master:7077 prova.py 0 0 1 1 0.5