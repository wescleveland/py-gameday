from logging import getLogger, Handler
import requests
from time import sleep

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}


class NullHandler(Handler):
    def emit(self, record):
        pass


class CONSTANTS:
    BASE = 'http://gd2.mlb.com/components/game/%LEAGUE%/'
    FETCH_TRIES = 50


class Fetcher:
    @classmethod
    def fetch(cls, url):
        for i in xrange(CONSTANTS.FETCH_TRIES):
            logger.debug('FETCH %s' % url)
            try:
                r = requests.get(url, headers=headers)
            except IOError:
                if i == CONSTANTS.FETCH_TRIES - 1:
                    logger.error('ERROR %s (max tries %s exhausted)' % (url, CONSTANTS.FETCH_TRIES))
                sleep(1)
                continue

            if r.status_code == 404:
                return ""
            else:
                return r.text
            break

logger = getLogger('gameday')
logger.addHandler(NullHandler())
