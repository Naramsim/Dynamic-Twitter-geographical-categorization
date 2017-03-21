"""
Provides helper functions to pleasantly print application states to the console. Strings are prepended by state-based colored tags and separated from those by a set of spaces.
"""

import colorclass


TAG_SUCCESS = "[+]"
TAG_FAIL = "[-]"
TAG_TENTATIVE = "[?]"
TAG_SPACING = "    "

def init():
    """
    Initializes the environment to allow ASCII color characters within the Windows cmd.
    """

    colorclass.Windows.enable()

def success(string, tag=TAG_SUCCESS):
    """
    Prints a string representing a succesful application state. The default "[+]" tag is color-coded green.

    :param string: The message to be printed.

    :prints: The formatted string containing the color coded tag and the message.
    """

    tag = colorclass.Color("{higreen}"+tag+"{/green}")
    log = tag+TAG_SPACING+string

    print(log)

def fail(string, tag=TAG_FAIL):
    """
    Prints a string representing a failed application state. The default "[-]" tag is color-coded red.

    :param string: The message to be printed.

    :prints: The formatted string containing the color coded tag and the message.
    """

    tag = colorclass.Color("{hired}"+tag+"{/red}")
    log = tag+TAG_SPACING+string

    print(log)

def tentative(string, tag=TAG_TENTATIVE):
    """
    Prints a string representing a tentative application state. The default "[+]" tag is color-coded yellow.

    :param string: The message to be printed.

    :prints: The formatted string containing the color coded tag and the message.
    """

    tag = colorclass.Color("{hiyellow}"+tag+"{/yellow}")
    log = tag+TAG_SPACING+string

    print(log)

init()
