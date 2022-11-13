import sqlite3

class Datastore:

    def __init__(self, db_file_name: str):

        self.connection = sqlite3.connect(db_file_name)
        self.cursor = self.connection.cursor()

        self.build_db()

    def __del__(self):
        self.connection.close()

    def build_db(self):
        self.cursor.execute(
            """
            CREATE TABLE rating_db(
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
        
        with open ("digital.csv", encoding="utf-8") as netflix_file:
            csv_reader = csv.DictReader(netflix_file, delimiter= ",")

            for record in csv_reader:
                if record["rating"]:
                    if record["rating"] not in self.get_all_ratings():
                        self.add_rating(record["rating"])
                    rating_id = self.get_rating_id(record["rating"])
                    if record["type","title","date_added","release_year","duration","description"]:
                        if record["type","title","date_added","release_year","duration","description"] not in self.get_all_shows():
                            self.add_show_table(record["type","title","date_added","release_year","duration","description"],rating_id)
                        
                    

                
            
                
        self.cursor.commit()

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

    def get_rating_id(self, name: str) -> int:
        self.cursor.execute(
            """
            SELECT rating_id 
            FROM rating_tb
            WHERE name = :name
            """,
            {
                "name":name
            }
        )
        
        results = self.cursor.fetchone()

        return results

    def add_rating(self, name):
        self.cursor.execute(
                    """
                    INSERT INTO rating_tb(name)
                    VALUES (:name)

                    """,
                {
                    "name":name[0]
                }
                    )
    def add_show_table(self, type, name, date_added, release_year, duration, description, rating_id):
        self.cursor.execute(
            """ 
            INSERT INTO show_tb
            values (:name,:type,:date_added,:release_year,:rating_id,:duration,:description)
            """,
            {
                "name":name,
                "type":type,
                "date_added":date_added,
                "release_year":release_year,
                "duration":duration,
                "description":description,
                "rating_id":rating_id

            }
        )
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