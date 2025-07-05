from parser.load_npi import NPI_Load
from parser.npi_maper import NPIDataFrameMapper
from db.mongo import GenericMongo
import os
from dotenv import load_dotenv

load_dotenv()

mongodb = GenericMongo(
        connection_string=os.getenv("MONGO_URL"),
        database_name=os.getenv("DATABASE_NAME"),
        collection_name=os.getenv("COLLECTION_NAME")
        )

load = NPI_Load("DAC_NationalDownloadableFile.csv")

mapper = NPIDataFrameMapper()
for i, chunk in enumerate(load.read_csv_in_chunks(chunk_size=10)):
        chunk = load.normalize_columns(chunk)
        for idx in range(len(chunk)):
            row_df = chunk.iloc[[idx]]
            mongodb.insert(row_df, unique_field='npi')
        break
exists_after_update = mongodb.find_npi('1003000126')
print(exists_after_update)
mongodb.close()