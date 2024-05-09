import argparse
import csv
import json
import re

import pandas as pd
import pymongo
from bson import ObjectId

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
database = mongo_client["QA"]
data = []
uniques = set()
yes_pattern = re.compile(r"yes(?!/no)", re.IGNORECASE)


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collections", type=str, help="The name of the collection(s) to use separated only by |",
                        required=True),
    parser.add_argument('--insert', type=argparse.FileType('r'), help="import report",
                        required=False)
    parser.add_argument('--file_mega', type=argparse.FileType('r'), help="import mega report", required=False)
    parser.add_argument('--user', type=str, help="list work done by user", required=False)
    parser.add_argument('--build_date', type=str, help="list all bugs on build date xx/xx/xxxx", required=False)
    parser.add_argument('--repeatable', action='store_true', help="list all repeatable bugs", required=False)
    parser.add_argument('--blocker', action='store_true', help="list all blocker bugs", required=False)
    parser.add_argument('--first', action='store_true', help="get first test case", required=False)
    parser.add_argument('--middle', action='store_true', help="get middle test case", required=False)
    parser.add_argument('--last', action='store_true', help="get last test case", required=False)
    parser.add_argument('--export_csv', type=str, help="exports report to csv", required=False)
    parser.add_argument('--verbose', action='store_true', help="prints number of effected rows", required=False)
    parser.add_argument('--very_verbose', action='store_true', help="prints each row  imported and queried",
                        required=False)
    parser.add_argument("--allow_duplicates", action='store_true', help="allows duplicates", required=False)
    return parser.parse_args()


def handle_arguments(args):
    query = {}
    is_verbose = args.verbose
    is_very_verbose = args.very_verbose

    for collection in args.collections.strip().split('|'):

        if args.insert is not None:
            do_import(database[collection], args.insert)

        if args.repeatable is not False:
            query["Repeatable?"] = {"$regex": yes_pattern}

        if args.blocker is not False:
            query["Blocker?"] = {"$regex": yes_pattern}

        if args.user is not None:
            query["Test Owner"] = args.user

        if args.build_date is not None:
            match = re.match(r"(\d{1,2})/(\d{1,2})", args.build_date)
            if match:
                month, day = match.groups()
                date_pattern = re.compile(r"(?=.*{})(?=.*{})".format(month, day))
                query["Build #"] = {"$regex": date_pattern}
            else:
                print("Date format not recognized")

        if args.first is not False:
            first_entry = database[collection].find_one()
            data.append(first_entry)

        if args.middle is not False:
            total = database[collection].count_documents({})
            middle = total // 2
            middle_entry_cursor = database[collection].find().skip(middle).limit(1)
            middle_entry = next(middle_entry_cursor, None)
            if middle_entry:
                data.append(middle_entry)

        if args.last is not False:
            last_entry = database[collection].find_one(sort=[("_id", -1)])
            data.append(last_entry)

        if len(query) > 0:
            do_query(database[collection], query, args.allow_duplicates)

        if args.export_csv is not None:
            file_name = args.export_csv
            pd.DataFrame(data).to_csv(file_name, header=True, index=False)
            print("Exported csv to: {}".format(file_name))

        if len(data) > 0:
            if is_very_verbose:
                for entry in data:
                    if entry is not None and (isinstance(entry, dict) or hasattr(entry, '__len__')):
                        filtered_entry = {key: value for key, value in entry.items() if not isinstance(value, ObjectId)}
                        print(json.dumps(filtered_entry, indent=4))
            if is_verbose:
                print("Total Documents affected: {len}, Collection: {collection}".format(len=len(data), collection=collection))


def do_query(collection, query, duplicates):
    for document in collection.find(query, {'_id': 0}):
        if duplicates:
            data.append(document)
        else:
            if tuple(document.items()) not in uniques:
                data.append(document)
                uniques.add(tuple(document.items()))


def do_import(collection, file):
    file_name = file.name
    if file_name.endswith(".csv") or file_name.endswith(".xlsx"):
        if file_name.endswith(".xlsx"):
            df = pd.read_excel(file_name)
            file_name = file_name[:-5]
            df.to_csv(file_name, header=True, index=False)
        with open(file_name, mode='r', encoding="utf8") as f:
            required_keys = ['Test #', 'Build #', 'Category', 'Test Case', 'Expected Result', 'Actual Result',
                             'Repeatable?', 'Blocker?', 'Test Owner']
            print("Inserted " + file.name)
            rows_to_insert = []
            for row in csv.DictReader(f):
                if all(key in row and row[key].strip() for key in required_keys):
                    rows_to_insert.append(row)

            if rows_to_insert:
                collection.insert_many(rows_to_insert)
            else:
                print("No valid rows to insert into the collection.")


if __name__ == '__main__':
    handle_arguments(arguments())
