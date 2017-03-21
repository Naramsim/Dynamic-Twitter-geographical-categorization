import argparse
import json
import re

from pyspark.sql import SparkSession

import booter
import log


def build_grid():
    """
    Builds the initial grid within the main map area, then used by the categorization algorithm.

    :prints log: When the grid has been correctly built.
    """

    data = _load_data("file:///opt/hdfs/URLCat/topics.fake.json")
    grid = _compute_grid(data)
    _save_grid(grid)

    log.success("grid built")

def _load_data(path="file:///opt/hdfs/URLCat/data.json"):
    """
    Loads the data from a .json file. See https://www.supergloo.com/fieldnotes/spark-sql-json-examples/ and http://stackoverflow.com/a/7889243/3482533.

    :param path: The path to the datafile on the server.

    :return: A DataFrame representing the .json file.

    :prints log: When the data has been succesfully loaded.
    """

    spark = SparkSession.builder.getOrCreate()
    json = booter.CONTEXT.textFile(path)
    data = spark.read.json(json)

    log.success("data read")
    return data

def _compute_grid(data):
    """
    Computes a grid within a specific area of the main map (possibly the entire area).

    :param data: The DataFrame representing the entire map.

    :return: A list of all the grid Tiles within the specific area containing at least one topic.
    """

    tiles = _split_area(booter.BOTTOM_LEFT, booter.TOP_RIGHT, booter.TILE_SIZE)
    computed_tiles = map(lambda tile: _compute_tile_topic(data, tile[0], tile[1]), tiles)
    computed_tiles = filter(None, computed_tiles)

    return computed_tiles

def _save_grid(grid):
    """
    Saves the grid to the redis database.

    :param grid: The list of the grid tiles.
    """

    for tile in grid:
        key = str(tile["coords"][0][0])+"X"+str(tile["coords"][0][1])
        booter.REDIS.set(key, json.dumps(tile))

def _split_area(bottomleft, topright, step):
    """
    Splits a square area in multiple square sub-Tiles.

    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.
    :param step: float representing the size of the sub-Tiles' edges.

    :return: A list of tuples containing two tuples representing the bottom-left and top-right vertex of each sub-Tile.
    """

    subtiles = []

    coord = bottomleft[0]
    subtile_xs = []
    while (coord < topright[0]):
        subtile_xs.append(coord)
        coord = round(coord+step, 2)

    coord = bottomleft[1]
    subtile_ys = []
    while (coord < topright[1]):
        subtile_ys.append(coord)
        coord = round(coord+step, 2)

    for x in subtile_xs:
        for y in subtile_ys:
            tile = ((x, y), (min(topright[0], round(x+step, 2)), min(topright[1], round(y+step, 2))))
            subtiles.append(tile)

    return subtiles

def _compute_tile_topic(data, bottomleft, topright):
    """
    Compute the most relevant topic in a specified square section (Tile) of an x,y coordinates plane. Empty tiles (without topics within them) are not returned.

    :param data: The data available within the x,y coordinates plane (or a subset of it).
    :param bottomleft: (x, y) tuple representing the bottom.left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "topics", a list of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile.

    :todo: Switch to a lat,long coordinates system.
    :prints debug: The coordinates specifying the Tile, and the computed most relevant topic within it.
    """

    tile = _filter_tile_data(data, bottomleft, topright)
    tile = _extract_tile_topic(tile)

    if tile:
        print("{}: {}".format(tile["coords"], tile["main"]))
        return tile

def _filter_tile_data(data, bottomleft, topright):
    """
    Create a new square Tile dict, filtering the data to only what is within the specified coordinates.

    :param data: The data from which to calculate the Tile.
    :param bottomleft: (x, y) tuple representing the bottom-left vertex of the Tile.
    :param topright: (x, y) tuple representing the top-right vertex of the Tile.

    :return: A dict representing a Tile, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the Tile; "data", a DataFrame containing all the data available within the Tile.
    """

    if (topright[0] == TOP_RIGHT[0]):
        x_filtered = data.filter(data.lat >= bottomleft[0]).filter(data.lat <= topright[0])
    else:
        x_filtered = data.filter(data.lat >= bottomleft[0]).filter(data.lat < topright[0])

    if (topright[1] == TOP_RIGHT[1]):
        xy_filtered = x_filtered.filter(data.lng >= bottomleft[1]).filter(data.lng <= topright[1])
    else:
        xy_filtered = x_filtered.filter(data.lng >= bottomleft[1]).filter(data.lng < topright[1])

    tile = {"coords": (bottomleft, topright), "data": xy_filtered}

    return tile

def _extract_tile_topic(tile):
    """
    Compute the overall topic of the provided tile.

    :param tile: The tile of which to compute the topic.

    :return: The dict representing the provided Tile, with the following properties added: "topics", a list of (topic, relevance) tuples containing the different topics and the relevance of each topic within the Tile; "main", a single (topic, relevance) tuple representing the most relevant topic within the Tile. If no topics are available within the Tile, False is returned instead of the dict.
    """

    raw_topics = tile["data"].select("topics")
    mapped_topics = raw_topics.rdd.flatMap(lambda rdd: rdd[0]).map(lambda topic: (topic.keyword, topic.weight))
    reduced_topics = mapped_topics.reduceByKey(lambda prv, nxt: round(prv+nxt, 2))

    tile["topics"] = reduced_topics.collect()

    if tile["topics"]:
        tile["main"] = max(tile["topics"], key=(lambda item: item[1]))
    else:
        tile = False

    return tile

def compute_area(bottomleft, topright):
    """
    Computes the most relevant topics in a specified area within the main map.

    :param bottomleft: (x, y) tuple representing the bottom-left vertex of the area.
    :param topright: (x, y) tuple representing the top-right vertex of the area.

    :return: A dict representing the area, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the area; "topics", a list of (topic, relevance) tuples containing the different topics and the relevance of each topic within the area; "main", a single (topic, relevance) tuple representing the most relevant topic within the area.
    """

    area = _filter_area_data(bottomleft, topright)
    area = _extract_area_topic(area)

    return area

def _filter_area_data(bottomleft, topright):
    """
    Create a new square area dict, filtering the data to only what is within the specified coordinates.

    :param bottomleft: (x, y) tuple representing the bottom-left vertex of the area.
    :param topright: (x, y) tuple representing the top-right vertex of the area.

    :return: A dict representing a area, with the following properties: "coords", a tuple containing the tuples representing the bottom-left and top-right vertices of the area; "data", a dict containing all the data available within the area.
    """

    area = {"coords": (bottomleft, topright), "data": []}

    i = bottomleft[0] - ((int(bottomleft[0]*MULT) % int(TILE_SIZE*MULT))/MULT)
    while (i < topright[0]):
        j = bottomleft[1] - ((int(bottomleft[1]*MULT) % int(TILE_SIZE*MULT))/MULT)
        while (j < topright[1]):
            key = str(i)+"X"+str(j)
            if key in grid:
                area["data"].append(json.loads(booter.REDIS.get(key)))
            j += TILE_SIZE
        i += TILE_SIZE

    return area

def _extract_area_topic(area):
    """
    Compute the overall topic of the provided area.

    :param area: The area of which to compute the topic.

    :return: The dict representing the provided area, with the following properties added: "topics", a list of (topic, relevance) tuples containing the different topics and the relevance of each topic within the area; "main", a single (topic, relevance) tuple representing the most relevant topic within the area. If no topics are available within the area, False is returned instead of the dict.
    """

    raw_topics = context.parallelize(map(lambda tile: tile["topics"], area["data"]))
    mapped_topics = raw_topics.flatMap(lambda topic: topic)
    reduced_topics = mapped_topics.reduceByKey(lambda prv, nxt: round(prv+nxt, 2))

    area["topics"] = reduced_topics.collect()
    del area["data"]

    if area["topics"]:
        area["main"] = max(area["topics"], key=(lambda item: item[1]))
    else:
        area = False

    return area
