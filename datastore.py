import sqlite3
from unicodedata import name
from unittest import result



class Datastore:
    def __init__(self):
        """
        intialise datastore by connecting to the sqlite db
        """
        db_file = "TTGames.db"
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
