import sqlite3
    


class csv:

    
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


