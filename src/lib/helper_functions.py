#!/usr/bin/env python
"""Module with miscellaneous utility functions.

This module contains a function to capture the current datetime stamp,
and configures logging format.

This module can be used to add more helper functions as needed.

"""

# built in python modules

from datetime import datetime
import logging
import sys


def _getToday():
    """Returns timestamp string"""
    return datetime.now().strftime("%Y%m%d%H%M%S")


def set_logger(__name__):
    """Configures logger for all modules and returns logger object"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(levelname)s|%(asctime)s|%(name)s|%(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    return logger
