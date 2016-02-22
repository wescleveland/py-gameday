#!/usr/bin/env python
from BeautifulSoup import BeautifulSoup
from lib import game, atbats, hitchart, players, store, CONSTANTS, Fetcher
import argparse
from time import strptime
from multiprocessing import Pool
import logging
import datetime
import MySQLdb
from ConfigParser import ConfigParser
from Queue import Queue


def csv(value):
    """Define csv type for argparse add_argument() for month's & day's."""
    return map(int, value.split(','))


def get_months(year, start=1):
    months = []

    url = '%syear_%4d/' % (CONSTANTS.BASE, year)
    soup = BeautifulSoup(Fetcher.fetch(url))
    for link in soup.findAll('a'):
        if link['href'].find('month') >= 0:
            month = int(link['href'].replace('month_', '').rstrip('/'))
            if month >= start:
                months.append(month)

    return months


def get_days(year, month, start=1):
    days = []

    url = '%syear_%4d/month_%02d/' % (CONSTANTS.BASE, year, month)
    soup = BeautifulSoup(Fetcher.fetch(url))
    for link in soup.findAll('a'):
        if link['href'].find('day') >= 0:
            try:
                day = int(link['href'].replace('day_', '').rstrip('/'))
            except:
                # sometimes gameday will have like a '26_bak' directory
                continue
            if day >= start:
                days.append(day)

    return days


def run_gid(gid, gametype):
    retries = 0
    while retries < 10:
        try:
            DB = store.Store()
            g = game.Game(gid)
            if (g.game_type != gametype):
                return

            g.save()
            game_id = g.game_id

            ab = atbats.AtBats(gid, game_id)
            ab.save()

            chart = hitchart.HitChart(gid, game_id)
            chart.save()

            batters = players.Batters(gid, game_id)
            batters.save()

            pitchers = players.Pitchers(gid, game_id)
            pitchers.save()

            DB.finish()
            break

        except Exception as e:
            print e
            retries += 1


if __name__ == '__main__':
    # argparse stuff, to define & capture commandline options
    opt = argparse.ArgumentParser(prog="Py-Gameday",
                                  description="Grabs MLB Gameday data",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    opt.add_argument("-y", "--year", default="2015", type=int,
                     required=True,
                     choices=range(2001, 2016),
                     metavar='YYYY',
                     help="Required. 4 digit year.",)
    # optional arg's
    opt.add_argument("-m", "--month", type=csv,
                     metavar='M,M',
                     help="1-2 digit month.",)
    opt.add_argument("-d", "--day",
                     type=csv,
                     metavar='D,D',
                     help="1-2 digit day.",)
    opt.add_argument("-l", "--league",
                     choices=['mlb', 'aaa', 'aax'],
                     default="mlb",
                     help="league abbreviation.",)
    opt.add_argument("-t", "--gametype",
                     choices=['R', 'S', 'A', 'F', 'D', 'L', 'W'],
                     default="R",
                     help="R=regular season, S=spring training,\
                           A=allstar game, F=wildcard, D=division series,\
                           L=league series, W=world series.",)
    opt.add_argument('-v', '--version',
                     action='version', version='%(prog)s 1.0.1')
    opt.add_argument("-e", "--errors", default="log.txt")
    opt.add_argument("-a", "--delta", action="store_true")
    opt.add_argument("-b", "--verbose", action="store_true")

    args = opt.parse_args()
    # end of the argparse details

    log = logging.getLogger('gameday')
    startday = 1
    startmonth = 1

    # here lies a hack for the strptime thread bug
    foo = strptime('30 Nov 00', '%d %b %y')

    # initial DB code
    try:
        DB = store.Store()
    except MySQLdb.Error, e:
        print 'Database connection problem- did you setup a db.ini? (error: %s)' % e
        raise SystemExit

    if args.delta:
        sql = 'SELECT year, month, day FROM last WHERE type = %s'
        res = DB.query(sql, [args.league])
        if len(res) == 0:
            print 'sorry, no delta information found'
            raise SystemExit
        else:
            args.year, startmonth, startday = [int(x) for x in res[0]]

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)s - %(levelname)s - %(message)s')

    if args.verbose:
        log.setLevel(logging.DEBUG)
        log.addHandler(logging.StreamHandler())
    else:
        log.setLevel(logging.ERROR)
        log.addHandler(logging.StreamHandler())

    logfilename = './' + args.errors
    filelog = logging.FileHandler(logfilename, 'a')
    filelog.setLevel(logging.ERROR)
    filelog.setFormatter(formatter)

    log.addHandler(filelog)

    CONSTANTS.BASE = CONSTANTS.BASE.replace('%LEAGUE%', args.league)
    url = '%syear_%4d/' % (CONSTANTS.BASE, args.year)
    try:
        soup = BeautifulSoup(Fetcher.fetch(url))
    except TypeError, e:
        print 'Could not fetch %s' % url
        raise SystemExit

    if args.month is None:
        if startmonth:
            months = get_months(args.year, startmonth)
        else:
            months = get_months(args.year)
    else:
        months = args.month

    pool = Pool(10)  # 10 worker processes

    for month in months:
        if args.day is None:
            if startday:
                days = get_days(args.year, month, startday)
            else:
                days = get_days(args.year, month)
        else:
            days = args.day

        month_url = '%smonth_%02d' % (url, month)
        month_soup = BeautifulSoup(Fetcher.fetch(month_url))

        for day in days:
            day_url = '%s/day_%02d' % (month_url, day)
            soup = BeautifulSoup(Fetcher.fetch(day_url))

            for link in soup.findAll('a'):
                if link['href'].find('gid_') >= 0:
                    gid = link['href'].rstrip('/')
                    pool.apply_async(run_gid, (gid, args.gametype,), callback=None)

    pool.close()
    pool.join()

    # update last after a day
    sql = 'DELETE FROM last WHERE type = %s;'
    DB.query(sql, [args.league])

    sql = 'INSERT INTO last (type, year, month, day) VALUES(%s, %s, %s, %s)'
    DB.query(sql, [args.league, args.year, month, days[-1]])
    DB.save()

    DB.finish()
