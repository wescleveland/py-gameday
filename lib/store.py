from . import *
import MySQLdb
import sys
from ConfigParser import ConfigParser
from warnings import simplefilter


class Store:
    def __init__(self, **args):
        parser = ConfigParser()
        parser.read('%s/db.ini' % sys.path[0])
        user = parser.get('db', 'user')
        password = parser.get('db', 'password')
        db = parser.get('db', 'db')
        port = int(parser.get('db', 'port'))
        host = parser.get('db', 'host')

        args = {
            'host': host,
            'port': port,
            'user': user,
            'passwd': password,
            'db': db,
            'charset': 'utf8'
        }

        self.db = MySQLdb.connect(**args)
        self.cursor = self.db.cursor()

    def save(self):
        self.db.commit()

    def finish(self):
        self.db.commit()
        self.db.close()

    def query(self, query, values=None):
        simplefilter("error", MySQLdb.Warning)

        try:
            res = self.cursor.execute(query, values)
            return self.cursor.fetchall()
        except (MySQLdb.Error, MySQLdb.Warning), e:
            if type(e.args) is tuple and len(e.args) > 1:
                msg = e.args[1]
            else:
                msg = str(e)
            logger.error('%s\nQUERY: %s\nVALUES: %s\n\n' % (msg, query, ','.join([unicode(v) for v in values])))
