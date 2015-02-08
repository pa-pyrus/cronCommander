# vim:fileencoding=utf-8:ts=8:et:sw=4:sts=4:tw=79

"""
tourney.py: query Tournament information and write to database

Copyright (c) 2015 Pyrus <pyrus at coffee dash break dot at>
See the file LICENSE for copying permission.
"""

from datetime import datetime
from hashlib import md5
from json import loads
from logging import getLogger
from os import environ
from pathlib import Path
from subprocess import call, DEVNULL

from sqlalchemy.orm.exc import NoResultFound

from database import Session
from database.models import Tournament

STRP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def update():
    logger = getLogger("cronjob.tourney")
    logger.info("Updating Tournament database...")

    repo_path = environ["TOURNEY_REPO"]
    repository = Path(repo_path)
    if not repository.is_dir():
        logger.critical("Repository path %s is not a valid directory.",
                        repository)
        raise NotADirectoryError(str(repository))

    logger.info("Pulling repository changes...")
    call(["git", "pull", "-q"],
         cwd=str(repository), stdout=DEVNULL, stderr=DEVNULL)

    tourney_base = repository / "tournaments"
    if not tourney_base.is_dir():
        logger.critical("Tournament base %s is not a valid directory.",
                        tourney_base)
        raise NotADirectoryError(str(tourney_base))

    logger.info("Reading tournament event files...")
    event_json_files = (json for
                        json in tourney_base.glob("**/*.json")
                        if json.is_file())

    logger.info("Updating tournament database...")
    session = Session()

    paths = list()
    for event_json_file in event_json_files:
        relative = event_json_file.relative_to(tourney_base)
        paths.append(str(relative))
        logger.debug("Handling file %s.", relative)
        with event_json_file.open() as event_json:
            raw = event_json.read()
            hash = md5(raw.encode("utf-8")).hexdigest()
            tourney_data = loads(raw)
        logger.debug("Read JSON data, file hash: %s.", hash)
        tourney_date = datetime.strptime(tourney_data["date"], STRP_FORMAT)

        try:
            tourney = (session.query(Tournament)
                              .filter(Tournament.path == str(relative)).one())
        except NoResultFound:
            tourney = Tournament(tourney_data["title"], tourney_date,
                                 tourney_data["winner"], tourney_data["mode"],
                                 tourney_data["url"],
                                 str(relative), hash)

            session.add(tourney)
            logger.debug("Created new tourney: {0}".format(tourney))
        else:
            # check if hash has changed
            if tourney.md5_hash != hash:
                tourney.md5_hash = hash
                tourney.title = tourney_data["title"]
                tourney.date = tourney_date
                tourney.winner = tourney_data["winner"]
                tourney.mode = tourney_data["mode"]
                tourney.url = tourney_data["url"]
                session.add(tourney)

    session.commit()

    # remove tournaments no longer in the repo
    obsolete_tourneys = (session.query(Tournament)
                                .filter(Tournament.path.notin_(paths)).all())
    for obsolete_tourney in obsolete_tourneys:
        logger.info("Purged tourney {0} which is no longer present in the "
                    "repository".format(obsolete_tourney))
        session.delete(obsolete_tourney)

    session.commit()
    session.close()
