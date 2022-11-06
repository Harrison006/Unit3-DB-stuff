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




# create methods

    def cret_show(self):
        """
        Creates a Show Table in the DB
        show_id: int
        name: str
        type: str
        date_added: str
        date_released: int
        duration: str
        description: str
        """
        self.cur.execute(
        """
        CREATE TABLE "Show" (
        "show_id"	INTEGER,
        "name"	TEXT NOT NULL,
        "TYPE"	TEXT NOT NULL,
        "date_added"	TEXT,
        "date_released"	INTEGER,
        "Duration"	TEXT,
        "Description"	TEXT,
        PRIMARY KEY("show_id" AUTOINCREMENT)
        )
        """
        )
        self.conn.commit()


    def cret_dir(self):
        """
        Creates Director Table
        dir_id = int
        name = str
        """
        self.cur.execute(
            """
            CREATE TABLE "director" (
            "dir_id"	INTEGER,
            "name"	TEXT NOT NULL,
            PRIMARY KEY("dir_id" AUTOINCREMENT)
            )
            """
        )
        self.conn.commit()

    def cret_rating(self):
        """
        Creates rating table
        rating_id = int
        name = str
        """
        self.cur.execute(
            """
            CREATE TABLE "rating" (
            "rating_id"	INTEGER,
            "name"	TEXT NOT NULL,
            PRIMARY KEY("dir_id" AUTOINCREMENT)
            )
            """
        )
        self.conn.commit()

    def cret_actor(self):
        """
        Creates Actor table
        actor_id = int
        name = str
        """
        self.cur.execute(
            """
            CREATE TABLE "actor" (
            "actor_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("actor_id" AUTOINCREMENT)
            )
            """
        )
        self.conn.commit()

    def cret_country(self):
        """
        Creates country table
        country_id = int
        name = str
        """
        self.cur.execute(
            """
            CREATE TABLE "country" (
            "country_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("country_id" AUTOINCREMENT)
            )
            """
        )

    def cret_catagory(self):
        """
        Creates catagory table
        cat_id = int
        name = str
        """
        self.cur.execute(
            """
            CREATE TABLE "catagory" (
            "cat_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("cat_id" AUTOINCREMENT)
            )
            """
        )

# get methods

# update methods

# delete methods
