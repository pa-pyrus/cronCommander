# vim:fileencoding=utf-8:ts=8:et:sw=4:sts=4:tw=79

"""
pastats.py: query PA Stats information and write to database

Copyright (c) 2015 Pyrus <pyrus at coffee dash break dot at>
See the file LICENSE for copying permission.
"""

from datetime import datetime, timedelta
from gzip import decompress
from itertools import chain
from json import loads
from logging import getLogger
from os import environ
from shelve import open as shelfopen
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import trueskill
# default values are fine, let's assume 0.3% draw chance
# also, use mpmath as backend
trueskill.setup(draw_probability=0.003, backend="mpmath")

from sqlalchemy.orm.exc import NoResultFound

from database import Session
from database.models import Game, Player


def update():
    logger = getLogger("cronjob.pastats")
    logger.info("Updating PAStats Winners database...")

    # calculate duration, at most seconds since start
    with shelfopen(environ["PASTATS_SHELF"], "c") as shelf:
        last = shelf.get("last", 0)
    reset = int(environ["PASTATS_RESET"])
    start = max(last, reset)

    reference = datetime(1970, 1, 1) + timedelta(seconds=start)
    dur_delta = datetime.utcnow() - reference
    duration = int(dur_delta.total_seconds())

    # create url we need for the query
    params = {"start": start, "duration": duration}
    winners_url = "http://pastats.com/report/winners"
    url = "{0}?{1}".format(winners_url, urlencode(params))
    logger.debug("PAStats URL: %s", url)

    # update start for next query
    start += duration
    # set it back again three hours so we can be sure to get all matches
    start -= (60 * 60 * 3)
    # save it for the next run
    with shelfopen(environ["PASTATS_SHELF"], "c") as shelf:
        shelf["last"] = start

    request = Request(url, headers={"Accept-Encoding": "gzip"})
    with urlopen(request) as response:
        data = response.read()
        if response.info().get("Content-Encoding") == "gzip":
            data = decompress(data)
            data = str(data, "utf-8")

    logger.info("Updating game database...")
    games = loads(data)
    session = Session()
    known_games = list(chain(*session.query(Game.gid).all()))

    for game in games:
        game_id = game["gameId"]
        # check only games we haven't seen yet
        if game_id in known_games:
            continue

        # for now only add games with exactly 2 teams and 1 player per team
        teams = game["teams"]
        if (len(teams) != 2
                or len(teams[0]["players"]) != 1
                or len(teams[1]["players"]) != 1):
            continue

        # start time is stored in ms
        game_start = game["startTime"] // 1000
        game_time = datetime(1970, 1, 1) + timedelta(seconds=game_start)

        p1 = teams[0]["players"][0]
        p2 = teams[1]["players"][0]

        p1_id, p1_name = p1["playerId"], p1["playerName"]
        p2_id, p2_name = p2["playerId"], p2["playerName"]

        del p1, p2

        # ignore matches vs anonymous players for now
        if p1_id == -1 or p2_id == -1:
            continue

        try:
            p1 = session.query(Player).filter(Player.pid == p1_id).one()
        except NoResultFound:
            p1 = Player(p1_id, p1_name, trueskill.Rating(), game_time)
            session.add(p1)
            logger.debug("Created new player: {0}".format(p1))

        if p1.name != p1_name:
            p1.name = p1_name
            session.add(p1)

        if p1.updated < game_time:
            p1.updated = game_time
            session.add(p1)

        try:
            p2 = session.query(Player).filter(Player.pid == p2_id).one()
        except NoResultFound:
            p2 = Player(p2_id, p2_name, trueskill.Rating(), game_time)
            session.add(p2)
            logger.debug("Created new player: {0}".format(p2))

        if p2.name != p2_name:
            p2.name = p2_name
            session.add(p2)

        if p2.updated < game_time:
            p2.updated = game_time
            session.add(p2)

        # check for winner or draw and update ratings
        if game["winner"] == -1:
            winner = None
            p1.skill, p2.skill = trueskill.rate_1vs1(p1.skill,
                                                     p2.skill,
                                                     drawn=True)
        elif teams[0]["teamId"] == game["winner"]:
            winner = p1
            p1.skill, p2.skill = trueskill.rate_1vs1(p1.skill,
                                                     p2.skill)
        else:
            winner = p2
            p2.skill, p1.skill = trueskill.rate_1vs1(p2.skill,
                                                     p1.skill)

        # add match
        session.add(Game(game_id, winner, p1, p2))

        # for the unlikely case we receive the same match twice:
        known_games.append(game_id)

    session.commit()
    session.close()

    logger.info("Received and parsed all new data, "
                "seen {0} matches".format(len(known_games)))
