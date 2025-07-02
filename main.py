import argparse
import os
from dotenv import load_dotenv
import pandas as pd
from parser.load_npi import NPI_Load
from db.mongo import NPI_Mongo

def parse_and_insert(zip_path, prefix, chunk_size=50):
    loader = NPI_Load(zip_path, prefix=prefix)
    npi_mongo = get_mongo()

    for i, chunk in enumerate(loader.read_csv_in_chunks(chunk_size=chunk_size)):
        chunk = loader.normalize_columns(chunk)
        for idx in range(len(chunk)):
            single_row_df = chunk.iloc[[idx]]
            result = npi_mongo.insert(single_row_df, update_cache=True)
            print(f"Inserted row {idx+1} of chunk {i+1}")
        break  # remove this if you want to process the whole file

    npi_mongo.close()


def find_npi(npi: str):
    npi_mongo = get_mongo()
    result = npi_mongo.find_by_npi(npi)
    collect = npi_mongo.get_all_as_dict()
    print(collect)
    if result:
        print(f"NPI {npi} found:\n{result}")
    else:
        print(f"NPI {npi} not found.")
    npi_mongo.close()


def get_mongo():
    load_dotenv()
    return NPI_Mongo(
        connection_string=os.getenv("MONGO_URL"),
        database_name=os.getenv("DATABASE_NAME"),
        collection_name=os.getenv("COLLECTION_NAME"),
        cache_dir=os.getenv("CACHE_DIR")
    )


def main():
    parser = argparse.ArgumentParser(description="NPI Data Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: parse
    parser_parse = subparsers.add_parser("parse", help="Parse and insert data from zip")
    parser_parse.add_argument("zip_path", type=str, help="Path to the NPPES zip file")
    parser_parse.add_argument("--prefix", default="npidata", help="File prefix inside the zip")
    parser_parse.add_argument("--chunk_size", type=int, default=50, help="Chunk size for parsing")

    # Command: find_npi
    parser_find = subparsers.add_parser("find_npi", help="Find provider by NPI number")
    parser_find.add_argument("npi", type=str, help="NPI number to search")

    args = parser.parse_args()

    if args.command == "parse":
        parse_and_insert(args.zip_path, args.prefix, args.chunk_size)
    elif args.command == "find_npi":
        find_npi(args.npi)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()