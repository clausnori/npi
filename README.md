## This module works which NPI (CVS / Pandas.DataFrame ) 

# Dependencies 

- Pandas 
- dotenv
- pymongo

# Setup MONGO_DB
1.MONGO_URL = Mongo Atlas Url / or Local
2.COLLECTION_NAME = NPI DATA Collection

Edit .env

## Parse NPI 


```
#Import Api
from parser.load_npi import NPI_Load
from db.mongo import GenericMongo
import os
from dotenv import load_dotenv

load_dotenv()


#Setup MONGO
mongodb = GenericMongo(
        connection_string=os.getenv("MONGO_URL"),
        database_name=os.getenv("DATABASE_NAME"),
        collection_name=os.getenv("COLLECTION_NAME")
        )


#Init Npi Loader 
load = NPI_Load("DAC_NationalDownloadableFile.csv")

#Interation in chank (you can work as you like, this is just an example )

#Read cvs in chank

for i, chunk in enumerate(load.read_csv_in_chunks(chunk_size=10)):
        #Normalize_columns name in Snake_Case
        chunk = load.normalize_columns(chunk)
        for idx in range(len(chunk)):
            #Get one DataFrame
            row_df = chunk.iloc[[idx]]
            mongodb.insert(row_df, unique_field='npi')
        break
#With this method we can check if a record and its data hash exist, to update the fields you need 

exists_after_update = mongodb.find_npi('1003000126')
print(exists_after_update)
mongodb.close()


#Or mongodb.exist('1003000126')

```