import sqlite3
import csv


class Datastore:
    def __init__(self, db_file_name: str):

        self.connection = sqlite3.connect(db_file_name)
        self.cursor = self.connection.cursor()

        self.build_db()
        self.populate_db()

    def __del__(self):
        self.connection.close()

    def build_db(self):

        self.cursor.execute(
            """
            CREATE TABLE rating_tb(
                rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL 
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE show_tb(
                show_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                type TEXT NOT NULL,
                date_added TEXT,
                release_year INTEGER NOT NULL CHECK(release_year > 1900),
                rating INTERGER,
                duration TEXT NOT NULL,
                description TEXT,
                FOREIGN KEY (rating) REFERENCES rating_tb(rating_id)
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE director_tb(
                dir_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE show_director(
                show_id TEXT NOT NULL,
                dir_id INTEGER NOT NULL,
                PRIMARY KEY (show_id, dir_id)
                FOREIGN KEY (show_id) REFERENCES show_tb(show_id)
                FOREIGN KEY (dir_id) REFERENCES director_tb(dir_id)
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE actor_tb(
                actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE cast_tb(
                show_id TEXT NOT NULL,
                actor_id INTEGER NOT NULL,
                PRIMARY KEY (show_id, actor_id)
                FOREIGN KEY (show_id) REFERENCES show_tb(show_id)
                FOREIGN KEY (actor_id) REFERENCES actor_tb(actor_id)
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE country_tb(
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE show_country_tb(
                show_id TEXT NOT NULL,
                country_id INTEGER NOT NULL,
                PRIMARY KEY (show_id, country_id)
                FOREIGN KEY (show_id) REFERENCES show_tb(show_id)
                FOREIGN KEY (country_id) REFERENCES country_tb(country_id)
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE category_tb(
                cat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE show_category_tb(
                show_id TEXT NOT NULL,
                cat_id INTEGER NOT NULL,
                PRIMARY KEY (show_id, cat_id)
                FOREIGN KEY (show_id) REFERENCES show_tb(show_id)
                FOREIGN KEY (cat_id) REFERENCES category_tb(cat_id)
            )
            """
        )

        self.connection.commit()

    def populate_db(self):

        with open("digital.csv", encoding="utf-8") as netflix_movies____5:
            csv_reader = csv.DictReader(netflix_movies____5, delimiter=",")

            for record in csv_reader:
                if record["show_id"]:
                    if record["rating"] not in self.get_all_ratings():
                        self.add_rating(record["rating"])
                    rating_id = self.get_rating_id(record["rating"])
                    show_id = record["show_id"]
                    print(show_id)

                    self.add_show_table(
                        record["show_id"],
                        record["type"],
                        record["title"],
                        record["date_added"],
                        record["release_year"],
                        record["duration"],
                        record["description"],
                        rating_id,
                    )
                    # add director
                    if record["director"] != "":
                        for director in record["director"].split(","):
                            if director not in self.get_all_directors():
                                self.add_director(director)
                            dir_id = self.get_director_id(director)
                            try:
                                self.add_show_director_table(show_id, dir_id)
                            except Exception:
                                with open(
                                    "error_log.txt", "a", encoding="utf-8"
                                ) as log:
                                    log.write(
                                        f"Duplicate of director {director} in show {show_id}\n"
                                    )
                    if record["cast"] != "":
                        for actor in record["cast"].split(","):
                            if actor not in self.get_all_cast():
                                self.add_actor(actor)
                            actor_id = self.get_actor_id(actor)
                            try:
                                self.add_cast_tb(show_id, actor_id)
                            except Exception:
                                with open(
                                    "error_log.txt", "a", encoding="utf-8"
                                ) as log:
                                    log.write(
                                        f"Duplicate of actor {actor} in show {show_id}\n"
                                    )
                    if record["country"] != "":
                        for country in record["country"].split(","):
                            if country not in self.get_all_countrys():
                                self.add_country(country)
                            country_id = self.get_country_id(country)
                            try:
                                self.add_show_country_tb(show_id, country_id)
                            except Exception:
                                with open(
                                    "error_log.txt", "a", encoding="utf-8"
                                ) as log:
                                    log.write(
                                        f"Duplicate of country {country} in show {show_id}\n"
                                    )
                    for category in record["listed_in"].split(","):
                        if category not in self.get_all_category():
                            self.add_category(category)
                        category_id = self.get_category_id(category)
                        try:
                            self.add_show_category(show_id, category_id)
                        except Exception:
                            with open("error_log.txt", "a", encoding="utf-8") as log:
                                log.write(
                                    f"Duplicate of category {category} in show {show_id}\n"
                                )

                self.connection.commit()

    def get_all_ratings(self):
        self.cursor.execute(
            """
            SELECT name
            FROM rating_tb
            """
        )
        results = self.cursor.fetchall()
        processed = []
        for results in results:
            processed.append(results[0])
        return processed

    def get_rating_id(self, name):
        self.cursor.execute(
            """
            SELECT rating_id 
            FROM rating_tb
            WHERE name = :name
            """,
            {"name": name},
        )

        results = self.cursor.fetchone()

        return results[0]

    def get_all_shows(self):
        self.cursor.execute(
            """
            SELECT *
            FROM show_tb
            """
        )
        results = self.cursor.fetchall()
        shows = []
        for result in results:
            shows.append(result[0])
        return shows

    def get_director_id(self, name):
        self.cursor.execute(
            """
            SELECT dir_id
            FROM director_tb
            WHERE name = :name
            """,
            {"name": name},
        )
        result = self.cursor.fetchone()
        return result[0]

    def get_all_directors(self):
        self.cursor.execute(
            """
            SELECT name 
            FROM director_tb
            """
        )
        results = self.cursor.fetchall()
        processed = []
        for results in results:
            processed.append(results[0])
        return processed

    def get_all_cast(self):
        self.cursor.execute(
            """
            SELECT name
            FROM actor_tb
            """
        )
        results = self.cursor.fetchall()
        processed = []
        for results in results:
            processed.append(results[0])
        return processed

    def get_actor_id(self, name):
        self.cursor.execute(
            """
            SELECT actor_id
            FROM actor_tb
            WHERE name = :name
            """,
            {"name": name},
        )
        result = self.cursor.fetchone()
        return result[0]

    def get_all_countrys(self):
        self.cursor.execute(
            """
            SELECT name 
            FROM country_tb
            """
        )
        results = self.cursor.fetchall()
        processed = []
        for results in results:
            processed.append(results[0])
        return processed

    def get_country_id(self, name):
        self.cursor.execute(
            """
            SELECT country_id
            FROM country_tb
            WHERE name = :name
            """,
            {"name": name},
        )
        result = self.cursor.fetchone()
        return result[0]

    def get_all_category(self):
        self.cursor.execute(
            """
            SELECT name 
            FROM category_tb
            """
        )
        results = self.cursor.fetchall()
        processed = []
        for results in results:
            processed.append(results[0])
        return processed

    def get_category_id(self, name):
        self.cursor.execute(
            """
            SELECT name
            FROM category_tb
            WHERE name = :name
            """,
            {"name": name},
        )
        result = self.cursor.fetchone()
        return result[0]

    def add_rating(self, name):
        self.cursor.execute(
            """
                    INSERT INTO rating_tb(name)
                    VALUES (:name)

                    """,
            {"name": name},
        )

    def add_show_table(
        self,
        show_id,
        type,
        name,
        date_added,
        release_year,
        duration,
        description,
        rating_id,
    ):
        self.cursor.execute(
            """ 
            INSERT INTO show_tb(show_id,title,type,date_added,release_year,rating,duration,description)
            values (:show_id,:name,:type,:date_added,:release_year,:rating_id,:duration,:description)
            """,
            {
                "show_id": show_id,
                "name": name,
                "type": type,
                "date_added": date_added,
                "release_year": release_year,
                "duration": duration,
                "description": description,
                "rating_id": rating_id,
            },
        )

    def add_director(self, name):
        self.cursor.execute(
            """
            INSERT INTO director_tb(name)
            VALUES (:name)
            """,
            {"name": name},
        )

    def add_show_director_table(self, show_id, dir_id):
        self.cursor.execute(
            """
            INSERT INTO show_director(show_id,dir_id)
            VALUES (:show_id,:dir_id)
            """,
            {"show_id": show_id, "dir_id": dir_id},
        )

    def add_actor(self, name):
        self.cursor.execute(
            """
            INSERT INTO actor_tb(name)
            VALUES(:name)
            """,
            {"name": name},
        )

    def add_cast_tb(self, show_id, actor_id):
        self.cursor.execute(
            """
            INSERT INTO cast_tb(show_id,actor_id)
            VALUES(:show_id,:actor_id)
            """,
            {"show_id": show_id, "actor_id": actor_id},
        )

    def add_country(self, name):
        self.cursor.execute(
            """
            INSERT INTO country_tb(name)
            VALUES(:name)
            """,
            {"name": name},
        )

    def add_show_country_tb(self, show_id, country_id):
        self.cursor.execute(
            """
            INSERT INTO show_country_tb(show_id,country_id)
            VALUES(:show_id,:country_id)
            """,
            {"show_id": show_id, "country_id": country_id},
        )

    def add_category(self, name):
        self.cursor.execute(
            """
            INSERT INTO category_tb(name)
            VALUES(:name)
            """,
            {"name": name},
        )

    def add_show_category(self, show_id, category_id):
        self.cursor.execute(
            """
            INSERT INTO show_category_tb(show_id,cat_id)
            VALUES(:show_id,:category_id)
            """,
            {"show_id": show_id, "category_id": category_id},
        )
