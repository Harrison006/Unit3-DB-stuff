import sqlite3
from unicodedata import name
from unittest import result




class Datastore:
    def __init__(self):
        """
        intialise datastore by connecting to the sqlite db
        """
        db_file = "netflix.db"

# create methods
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def main(self):
    database = "netflix.db"

    cret_show = """
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


    cret_dir ="""
            CREATE TABLE "director" (
            "dir_id"	INTEGER,
            "name"	TEXT NOT NULL,
            PRIMARY KEY("dir_id" AUTOINCREMENT)
            )
            """

    cret_rating ="""
            CREATE TABLE "rating" (
            "rating_id"	INTEGER,
            "name"	TEXT NOT NULL,
            PRIMARY KEY("dir_id" AUTOINCREMENT)
            )
            """

    cret_actor ="""
            CREATE TABLE "actor" (
            "actor_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("actor_id" AUTOINCREMENT)
            )
            """

    cret_country ="""
            CREATE TABLE "country" (
            "country_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("country_id" AUTOINCREMENT)
            )
            """

    cret_catagory ="""
            CREATE TABLE "catagory" (
            "cat_id"	INTEGER,
            "name"	TEXT,
            PRIMARY KEY("cat_id" AUTOINCREMENT)
            )
            """
    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, cret_show,cret_actor,cret_catagory,cret_country,cret_dir,cret_rating,cret_show)
    else:
        print("uh oh no db lol")


if __name__ == '__main__':
    main()

# get methods

# update methods

# delete methods
