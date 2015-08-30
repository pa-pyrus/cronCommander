# vim:fileencoding=utf-8:ts=8:et:sw=4:sts=4:tw=79

"""
leaders.py: query leaderboard information and write to database

Copyright (c) 2015 Pyrus <pyrus at coffee dash break dot at>
See the file LICENSE for copying permission.
"""

from datetime import datetime
from logging import getLogger
from http.client import HTTPSConnection
from json import loads, dumps
from os import environ
from urllib.request import Request, urlopen

from database import Session
from database.models import LeaderBoardEntry, UberAccount

LASTMATCH_FORMAT = "%Y-%m-%d %H:%M:%SZ"
UBERENT_HOSTNAME = "4.uberent.com"
GAME_TYPES = {"Vanilla": "Ladder1v1", "Titans": "PAExpansion1:Ladder1v1"}
LEAGUES = {"Uber": 1, "Platinum": 2, "Gold": 3, "Silver": 4, "Bronze": 5}


def update_leaderboard(game, league, ticket, session):
    logger = getLogger("cronjob.leaders")
    logger.info("Updating {0} league for {1}...".format(league, game))

    game_type = GAME_TYPES[game]
    league_id = LEAGUES[league]

    url = ("/MatchMaking/GetRankLeaderboard?TitleId=4"
           "&GameType={0}&Rank={1}").format(game_type, league_id)
    logger.debug("Using URL {0}".format(url))

    connection = HTTPSConnection(UBERENT_HOSTNAME)
    connection.request("GET", url)

    # exceptions are propagated
    response = connection.getresponse()
    raw_data = response.read()
    leaderboard = loads(str(raw_data, "utf-8"))

    entries = leaderboard["LeaderboardEntries"]

    # we need to update uberaccounts first
    logger.info("Updating UberAccounts...")
    uberids = [entry["UberId"] for entry in entries]

    query = "&UberIds=".join(uberids)
    url = "/GameClient/UserNames?UberIds=" + query

    connection.request("GET", url, headers={"X-Authorization": ticket})
    response = connection.getresponse()
    raw_data = response.read()
    usernames = loads(str(raw_data, "utf-8"))
    uberusers = usernames["Users"]
    for uid, names in uberusers.items():
        uname = names["UberName"]
        dname = names["TitleDisplayName"]

        uberaccount = UberAccount(uname, uid, dname, None)

        # while we're at it, query pastats ID
        request = Request("http://pastats.com/report/"
                          "getplayerid?ubername={0}".format(uname))

        # exceptions are propagated
        with urlopen(request) as response:
            raw_data = response.read()

            pid = loads(str(raw_data, "utf-8"))
            if pid != -1:
                uberaccount.pid = pid
                logger.debug("Added PAStats ID for "
                             "UberAccount: {0}".format(uberaccount))

        session.merge(uberaccount)

    for rank in range(len(entries)):
        lbentry = entries[rank]
        last_match = datetime.strptime(
            lbentry["LastMatchAt"], LASTMATCH_FORMAT)
        dbentry = LeaderBoardEntry(
            game, league, rank + 1, lbentry["UberId"], last_match)

        # insert or update entry
        session.merge(dbentry)


def update():
    logger = getLogger("cronjob.leaders")
    logger.info("Updating Uber Leaderboard entries...")

    connection = HTTPSConnection(UBERENT_HOSTNAME)
    logger.debug("Uberent Host: %s", connection.host)

    logger.info("Requesting session ticket...")
    login_params = dumps({"TitleId": 4,
                          "AuthMethod": "UberCredentials",
                          "UberName": environ["UBERENT_UBERNAME"],
                          "Password": environ["UBERENT_PASSWORD"]})
    logger.debug("Encoded Ubernet login parameters.")

    connection.request("POST", "/GC/Authenticate",
                       body=login_params,
                       headers={"Content-Type": "application/json"})

    # exceptions are propagated
    response = connection.getresponse()
    raw_data = response.read()

    ticket = loads(str(raw_data, "utf-8"))
    logger.info("Got session ticket {0}.".format(ticket["SessionTicket"]))
    ticket = ticket["SessionTicket"]

    session = Session()

    logger.info("Requesting Leaderboard entries...")
    for game in GAME_TYPES.keys():
        for league in LEAGUES.keys():
            update_leaderboard(game, league, ticket, session)

    session.commit()
    session.close()
