from pyspark import SparkContext
from pyspark.sql import SparkSession

context = SparkContext()
context.setLogLevel("WARN")

def createTile(data, bottomleft, topright):
    """
    Create a new square Tile dict, filtering the data to only what is within the specified coordinates.

    :param data: The data from which to calculate the Tile.
    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "data", a DataFrame containing all the data available within the Tile.
    """

    xFiltered = data.filter(data.lat >= bottomleft[0]).filter(data.lat <= topright[0])
    xyFiltered = xFiltered.filter(data.lng >= bottomleft[1]).filter(data.lng <= topright[1])
    
    tile = {"coords": (bottomleft, topright), "data": xyFiltered}

    return tile

def computeTopic(tile):
    """
    Compute the overall topic of the provided tile. Each topic is given the same weight of 1.

    :param tile: The tile of which to compute the topic.

    :return: The dict representing the provided Tile, with the following properties added: "topics", a List of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile, or False if no topics are available within the Tile.

    :todo: Add weighting to topics, possibly by directly counting the weight available in the dataset.
    """

    rawTopics = tile["data"].select("topics")
    mappedTopics = rawTopics.rdd.flatMap(lambda x: x[0]).map(lambda topic: (topic.keyword, topic.weight))
    reducedTopics = mappedTopics.reduceByKey(lambda prv, nxt: round(prv+nxt, 2))
    tile["topics"] = reducedTopics.collect()
    print(tile["data"].select("topics").collect())
    if tile["topics"]:
        tile["main"] = max(tile["topics"], key=(lambda item: item[1]))
    else:
        tile["main"] = False

    return tile

def computeTileTopic(data, bottomleft, topright):
    """
    Compute the most relevant topic in a specified square section (Tile) of an x,y coordinates plane.

    :param data: The data available within the x,y coordinates plane (or a subset of it).
    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "data", a DataFrame containing all the data available within the Tile; "topics", a List of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile, or False if no topics are available within the Tile.

    :todo: Switch to a lat,long coordinates system.
    :prints debug: The coordinates specifying the Tile, and the computed most relevant topic within it.
    """

    tile = createTile(data, bottomleft, topright)
    tile = computeTopic(tile)

    print("{}: {}".format(tile["coords"], tile["main"]))
    return tile

def splitTile(bottomleft, topright, step):
    """
    Split a square Tile in multiple square sub-Tiles.

    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.
    :param step; float representing the size of the sub-Tiles' edges.

    :return: A list of tuples containing two tuples representing the bottom-left and top-right vertex of each sub-Tile.
    """

    subtiles = []

    coord = bottomleft[0]
    subtileXs = []
    while (coord < topright[0]):
        subtileXs.append(coord)
        coord = round(coord+step, 2)

    coord = bottomleft[1]
    subtileYs = []
    while (coord < topright[1]):
        subtileYs.append(coord)
        coord = round(coord+step, 2)

    for x in subtileXs:
        for y in subtileYs:
            tile = ((x, y), (min(topright[0], round(x+step, 2)), min(topright[1], round(y+step, 2))))
            subtiles.append(tile)

    return subtiles

spark = SparkSession.builder.getOrCreate()
jsonRDD = context.wholeTextFiles("file:///opt/hdfs/URLCat/topics.fake.json").map(lambda x: x[1]) # https://www.supergloo.com/fieldnotes/spark-sql-json-examples/ and http://stackoverflow.com/a/7889243/3482533
data = spark.read.json(jsonRDD)
data.printSchema()

bottomleft = (0.75, 0.75)
topright = (1, 1)
step = 0.05

computeTileTopic(data, bottomleft, topright)

subtiles = []
subtiles = splitTile(bottomleft, topright, step)
computed = map(lambda tile: computeTileTopic(data, tile[0], tile[1]), subtiles)
print(list(computed))
