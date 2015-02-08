# vim:fileencoding=utf-8:ts=8:et:sw=4:sts=4:tw=79

"""
uberent.py: query Uberent information and write to database

Copyright (c) 2015 Pyrus <pyrus at coffee dash break dot at>
See the file LICENSE for copying permission.
"""

from datetime import datetime
from json import loads, dumps
from logging import getLogger
from os import environ
from urllib.request import Request, urlopen

from sqlalchemy.orm.exc import NoResultFound

from database import Session
from database.models import Patch

UBERENT_URLROOT = "https://4.uberent.com"


def update():
    logger = getLogger("cronjob.uberent")
    logger.info("Updating Uberent Patch database...")
    logger.debug("Uberent URL: %s", UBERENT_URLROOT)

    logger.info("Requesting session ticket...")
    login_params = bytes(dumps({"TitleId": 4,
                                "AuthMethod": "UberCredentials",
                                "UberName": environ["UBERENT_UBERNAME"],
                                "Password": environ["UBERENT_PASSWORD"]}),
                         "ascii")
    logger.debug("Encoded Ubernet login parameters.")

    request = Request(UBERENT_URLROOT + "/GC/Authenticate")
    request.add_header("Content-Type", "application/json")
    request.data = login_params

    # exceptions are propagated
    with urlopen(request) as response:
        data = response.read()

    ticket = loads(str(data, "utf-8"))
    logger.debug("Got session ticket {0}.".format(ticket["SessionTicket"]))

    logger.info("Requesting Stream information...")
    request = Request(UBERENT_URLROOT + "/Launcher/ListStreams?Platform=Linux")
    request.add_header("X-Authorization", ticket["SessionTicket"])

    # exceptions are propagated
    with urlopen(request) as response:
        data = response.read()

    stream_data = loads(str(data, "utf-8"))
    streams = stream_data["Streams"]
    logger.info("Got {0} streams, updating database.".format(len(streams)))
    logger.debug("Streams: {0}".format(streams))

    session = Session()

    updated = datetime.utcnow().replace(second=0, microsecond=0)
    for stream in streams:
        try:
            patch = (session.query(Patch)
                            .filter(Patch.name == stream["StreamName"]).one())
        except NoResultFound:
            patch = Patch(stream["StreamName"], stream["BuildId"],
                          stream["Description"], updated)
            session.add(patch)
            logger.debug("Created new patch: {0}".format(patch))
        else:
            # check if BuildId has changed
            if patch.build != stream["BuildId"]:
                patch.build = stream["BuildId"]
                patch.description = stream["Description"]
                patch.updated = updated
                session.add(patch)

    session.commit()
    session.close()
