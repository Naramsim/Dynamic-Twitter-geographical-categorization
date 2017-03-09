from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode

def getSquare(data, topright, bottomleft):
    square = data.filter(data.lat >= bottomleft[0]).filter(data.lat <= topright[0]).filter(data.lng >= bottomleft[1]).filter(data.lng <= topright[1])
    return {"coords": (topright,bottomleft), "data": square}

def setTopic(square):
    topics = square["data"].select('topics').rdd.flatMap(lambda x: x[0]).map(lambda x: (x.keyword, 1))
    counted = topics.reduceByKey(lambda prv,nxt: prv+nxt)
    square["counted"] = counted.collect()
    if square["counted"]:
        square["topic"] = max(square["counted"],key=lambda item:item[1])
    else:
        square["topic"] = "nodata"
    return square

def compute(data, topright, bottomleft):
    square = getSquare(data, topright, bottomleft)
    square = setTopic(square)
    print("{}: {}".format(square["coords"], square["topic"]))
    return square

sc = SparkContext()
sc.setLogLevel("WARN")
spark = SparkSession.builder.getOrCreate()
data = spark.read.json('file:///opt/hdfs/URLCat/topics.json')
topright = (1,1)
bottomleft = (0,0)
step = 0.5
squares = []
# s0, s1, s2, s3 = bottomleft, (topright[0], bottomleft[1]), topright, (bottomleft[0], topright[1])

#compute(data, topright, bottomleft)

def calcXs():
    first = bottomleft[0]
    arr = []
    while(first < topright[0]):
        arr.append(first)
        first = first + step
    return arr

def calcYs():
    first = bottomleft[0]
    arr = []
    while(first < topright[0]):
        arr.append(first)
        first = first + step
    return arr

xs = calcXs()
ys = calcYs()

for x in xs:
    for y in ys:
        squares.append(((x,y), (x+step, y+step)))

a = map(lambda x: compute(data, x[1], x[0]), squares)
print(list(a))