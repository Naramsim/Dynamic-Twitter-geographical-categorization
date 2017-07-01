### Dynamic Twitter geographical categorization

> A map-reduce implementation for the categorization of Twitter tweets within dynamic geographical boundaries.

### Build

Docker and Compose has to be locally installed. To create and run a master node with two workers run the command below.

```
./scripts/up
```

### Gather data

```
docker run -it -v ${PWD}/share:/app node bash
node /app/app/generators/generator.twitter.js
# stop with Ctrl+C
```

### Run APIs

```
./scripts/attach
cd /opt/hdfs/app
spark-submit server.py bottom_left_lng bottom_left_lat top_right_lng top_right_lat size [-g]
# i.e.: spark-submit server.py 9 36 17 48 1 -g will parse data for Italy
```
