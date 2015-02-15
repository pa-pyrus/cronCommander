#!/usr/bin/env python3
# vim:fileencoding=utf-8:ts=8:et:sw=4:sts=4:tw=79

"""
cronjob.py: main implementation for the Commander cronjob

Copyright (c) 2015 Pyrus <pyrus at coffee dash break dot at>
See the file LICENSE for copying permission.
"""

from concurrent.futures import ProcessPoolExecutor, wait
from os import environ

import logging
import logging.handlers

import pastats
import tourney
import uberent
import leaders

# set up logging first
LOG_FORMAT = "{asctime} {levelname}({name}): {message}"
rfh = logging.handlers.RotatingFileHandler(environ["LOGFILE"],
                                           maxBytes=131027,  # 128kB
                                           backupCount=3)
logging.basicConfig(level=logging.INFO, handlers=(rfh,),
                    format=LOG_FORMAT, style="{")

logger = logging.getLogger("cronjob")

logger.info("Running cronCommander...")

if "DBG_CRON_COMMANDER" in environ:
    logger.setLevel(logging.DEBUG)
    logger.debug("Enabled debug output")

logger.info("Spawning processes...")
with ProcessPoolExecutor(max_workers=1) as executor:
    futures = {"pastats": executor.submit(pastats.update),
               "uberent": executor.submit(uberent.update),
               "leaders": executor.submit(leaders.update),
               "tourney": executor.submit(tourney.update)}

# wait for all processes
logger.info("Waiting for processes...")
wait(futures.values())
logger.info("Processes finished...")
logger.info("Exiting...")
