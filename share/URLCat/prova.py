import argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame


BOTTOM_LEFT = None
TOP_RIGHT = None
TILE_SIZE = None

def parse():
    """
    Parse the given arguments into global variables.
    """

    global BOTTOM_LEFT
    global TOP_RIGHT
    global TILE_SIZE

    parser = argparse.ArgumentParser(description="compute the most relevant topics in a custom grid within a square geographic area", formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=30))
    parser.add_argument("-v", "--version", action="version", version="0.0.0")

    parser.add_argument("bottomleftx", type=_positive_float, help="the x coordinate of the bottom-left vertex of the main geographic area")
    parser.add_argument("bottomlefty", type=_positive_float, help="the y coordinate of the bottom-left vertex of the main geographic area")
    parser.add_argument("toprightx", type=_positive_float, help="the x coordinate of the top-right vertex of the main geographic area")
    parser.add_argument("toprighty", type=_positive_float, help="the y coordinate of the top-right vertex of the main geographic area")
    parser.add_argument("tilesize", type=_positive_float, help="the size of each square tile that form the grid within the main geographic area")

    args = parser.parse_args()

    BOTTOM_LEFT = (args.bottomleftx, args.bottomlefty)
    TOP_RIGHT = (args.toprightx, args.toprighty)
    TILE_SIZE = args.tilesize

def _positive_float(value):
    """
    Check that the provided float value is positive.

    :return: The unchanged float value.
    """

    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("number must be positive")

    return number

def setGetContext():
    """
    Set the spark environment.

    :return: The current SparkContext.
    """

    context = SparkContext()
    context.setLogLevel("WARN")

    return context

def loadData(context):
    """
    Loads the data from a .json file. See https://www.supergloo.com/fieldnotes/spark-sql-json-examples/ and http://stackoverflow.com/a/7889243/3482533.

    :param context: The current SparkContext.

    :return: A DataFrame representing the .json file.
    :prints debug: The parsed data's schema.
    """

    spark = SparkSession.builder.getOrCreate()
    #json = context.wholeTextFiles("file:///opt/hdfs/URLCat/topics.json").map(lambda txt: txt[1])
    json = context.textFile("file:///opt/hdfs/URLCat/topics.json")
    data = spark.read.json(json)
    data.show()

    data.printSchema()
    return data

def createTileDF(data, bottomleft, topright):
    """
    Create a new square Tile dict, filtering the data to only what is within the specified coordinates.

    :param data: The data from which to calculate the Tile.
    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "data", a DataFrame containing all the data available within the Tile.

    :todo: Specify this is valid with DataFrames.
    """

    if (topright[0] == TOP_RIGHT[0]):
        xFiltered = data.filter(data.lat >= bottomleft[0]).filter(data.lat <= topright[0])
    else:
        xFiltered = data.filter(data.lat >= bottomleft[0]).filter(data.lat < topright[0])
    if (topright[1] == TOP_RIGHT[1]):
        xyFiltered = xFiltered.filter(data.lng >= bottomleft[1]).filter(data.lng <= topright[1])
    else:
        xyFiltered = xFiltered.filter(data.lng >= bottomleft[1]).filter(data.lng < topright[1])

    tile = {"coords": (bottomleft, topright), "data": xyFiltered}

    return tile

def computeTopicDF(tile):
    """
    Compute the overall topic of the provided tile. Each topic is given the same weight of 1.

    :param tile: The tile of which to compute the topic.

    :return: The dict representing the provided Tile, with the following properties added: "topics", a List of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile, or False if no topics are available within the Tile.

    :todo: Specify this is valid with DataFrames.
    """

    print(tile["data"])
    rawTopics = tile["data"].select("topics")
    mappedTopics = rawTopics.rdd.flatMap(lambda rdd: rdd[0]).map(lambda topic: (topic.keyword, topic.weight))
    reducedTopics = mappedTopics.reduceByKey(lambda prv, nxt: round(prv+nxt, 2))

    
    tile["topics"] = reducedTopics.collect()

    if tile["topics"]:
        tile["main"] = max(tile["topics"], key=(lambda item: item[1]))
    else:
        tile = False

    return tile

def computeTileTopic(data, bottomleft, topright):
    """
    Compute the most relevant topic in a specified square section (Tile) of an x,y coordinates plane.

    :param data: The data available within the x,y coordinates plane (or a subset of it).
    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "data", a DataFrame containing all the data available within the Tile; "topics", a List of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile, or False if no topics are available within the Tile.

    :todo: Switch to a lat,long coordinates system. Specify this is valid with DataFrames.
    :prints debug: The coordinates specifying the Tile, and the computed most relevant topic within it.
    """

    tile = createTileDF(data, bottomleft, topright)
    tile = computeTopicDF(tile)

    if tile:
        print("{}: {}".format(tile["coords"], tile["main"]))
        return tile

def createArea(grid, bottomleft, topright):
    """
    """

    area = {"coords": (bottomleft, topright), "data": []}

    i = bottomleft[0]-(bottomleft[0]%TILE_SIZE)
    while (i <= topright[0]):
        j = bottomleft[1]-(bottomleft[1]%TILE_SIZE)
        while (j <= topright[1]):
            key = str(i)+"X"+str(j)
            if key in grid: 
                area["data"].append(grid[key])
            j += TILE_SIZE
        i += TILE_SIZE
            
    return area

def computeTopicDict(context, tile):
    """
    """

    rawTopics = context.parallelize(map(lambda x: x["topics"], tile["data"]))
    mappedTopics = rawTopics.flatMap(lambda x: x)#.map(lambda topic: (topic[0], topic[1]))
    reducedTopics = mappedTopics.reduceByKey(lambda prv, nxt: round(prv+nxt, 2))
    print(reducedTopics.collect())
    
    tile["topics"] = reducedTopics.collect()

    if tile["topics"]:
        tile["main"] = max(tile["topics"], key=(lambda item: item[1]))
    else:
        tile = False

    return tile

def computeAreaTopic(context, grid, bottomleft, topright):
    """
    """

    area = createArea(grid, bottomleft, topright)
    area = computeTopicDict(context, area)

    return area

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

def computeGrid(data):
    """
    """

    subtiles = splitTile(BOTTOM_LEFT, TOP_RIGHT, TILE_SIZE)
    computed = map(lambda tile: computeTileTopic(data, tile[0], tile[1]), subtiles)
    computed = filter(None, computed)

    return computed

def computeArea(context, grid, bottomleft, topright):
    """
    """

    computed = computeAreaTopic(context, grid, bottomleft, topright)

    return computed

def delDF(obj):
    obj.pop('data', None)
    return obj

def GridToDict(context, grid):
    """
    """

    dictionary = {}
    for tile in grid:
        key = str(tile["coords"][0][0])+"X"+str(tile["coords"][0][1])
        dictionary[key] = tile

    return dictionary


parse()

context = setGetContext()
data = loadData(context)

grid = computeGrid(data)
grid = GridToDict(context, grid)
area = computeArea(context, grid, (0.5, 0.5), (1, 1))

#print(grid)
#print("-")
#print(area)
