# main program
import csv
import pprint

pp = pprint.PrettyPrinter(indent=4)

with open("netflix_titles.csv", encoding="utf-8") as netflix_file:
    csv_reader = csv.DictReader(netflix_file, delimiter=",")

    for row in csv_reader:
        pp.pprint(row["rating"])
