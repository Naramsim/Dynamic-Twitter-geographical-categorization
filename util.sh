hdfs dfs -copyFromLocal /opt/hdfs/URLCat/topics.fake.json /topics.fake.json

spark-submit --conf spark.driver.memory=2g --master spark://master:7077 categorize.py 0 0 1 1 0.5

spark-submit server.py 9 36 17 48 1 -g
spark-submit server.py bottom_left_lng bottom_left_lat top_right_lng top_right_lat 1 -g