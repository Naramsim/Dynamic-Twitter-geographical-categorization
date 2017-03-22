import argparse
import redis

from pyspark import SparkContext


GENERATE = False

BOTTOM_LEFT = None
TOP_RIGHT = None
TILE_SIZE = None

REDIS = None
CONTEXT = None

MULT = 10000.0

def parse():
    """
    Parses the given arguments into global variables.
    """

    global GENERATE
    global BOTTOM_LEFT
    global TOP_RIGHT
    global TILE_SIZE

    parser = argparse.ArgumentParser(description="compute the most relevant topics in a custom grid within a square geographic area", formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=30))
    parser.add_argument("-v", "--version", action="version", version="0.0.0")

    parser.add_argument("bottomleftx", type=_positive_float, help="the x coordinate of the bottomleft vertex of the main geographic area")
    parser.add_argument("bottomlefty", type=_positive_float, help="the y coordinate of the bottomleft vertex of the main geographic area")
    parser.add_argument("toprightx", type=_positive_float, help="the x coordinate of the topright vertex of the main geographic area")
    parser.add_argument("toprighty", type=_positive_float, help="the y coordinate of the topright vertex of the main geographic area")
    parser.add_argument("tilesize", type=_positive_float, help="the size of each square tile that forms the grid within the main geographic area")
    parser.add_argument("-g", "--generate", action="store_true", help="generate a new grid in the specified area")

    args = parser.parse_args()

    GENERATE = args.generate
    BOTTOM_LEFT = (args.bottomleftx, args.bottomlefty)
    TOP_RIGHT = (args.toprightx, args.toprighty)
    TILE_SIZE = args.tilesize

def init_redis(host="127.0.0.1", port=6379):
    """
    Connects to the redis database.

    :param host: The IP of the server hosting redis.
    :param port: The server port used by the redis service.
    """

    global REDIS

    REDIS = redis.StrictRedis(host=host, port=port, db=0)
    if GENERATE:
        REDIS.flushdb()

def init_context():
    """
    Instantiates the SparkContext to the redis database.
    """

    global CONTEXT

    CONTEXT = SparkContext()
    CONTEXT.setLogLevel("WARN")

def _positive_float(value):
    """
    Checks that the provided float value is positive.

    :param value: The value to be checked.

    :return: The unchanged float value.

    :raise ArgumentTypeError: If the float value is negative.
    """

    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("number must be positive")

    return number
