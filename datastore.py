import sqlite3
from unicodedata import name
from unittest import result




class Datastore:
    def __init__(self, db_file_name: str):

        self.conn = sqlite3.connect(netflix.db)
        self.cur = self.conn.cursor()


        self.build_db()

    def __del__(self):
        """
        writes data from cache to drive then closes connection
        """
        self.conn.close()


    def build_db(self):
        """
        Makes DB file with tables
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
        """
        Rating table made
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
        self.cur.execute(
            """
            CREATE TABLE "director" (
            "dir_id"	INTEGER,
            "name"	TEXT NOT NULL,
            PRIMARY KEY("dir_id" AUTOINCREMENT)
            )
            """
        )
        """
        Makes Director Table
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
        """
        Makes Show table
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
        """
        Makes Country table
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
        """
        Makes Catagory table
        """
        
        # Join tables

        self.cur.execute(
            """
            CREATE TABLE show_director(
                show_id TEXT NOT NULL,
                dir_id INTEGER NOT NULL,
                PRIMARY KEY (show_id, dir_id)
                FOREIGN KEY (show_id) REFERENCES Show(show_id)
                FOREIGN KET (dir_id) REFERENCES director(dir_id)
            )
            """
        )

# create methods


# get methods

# update methods

# delete methods
