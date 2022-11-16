import os
from unittest import result
from datastore import Datastore

db_file = "netflix.db"

os.remove(db_file)

db = Datastore(db_file)


db.populate_db()
