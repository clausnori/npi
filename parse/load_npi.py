import re
import zipfile
import pandas as pd
from pandas import DataFrame

class NPI_Load:
    def __init__(self, zip_path, prefix):
        self.zip_path = zip_path
        self.prefix = prefix.lower()
        self.csv_filename = None

        # Find nipdata
        with zipfile.ZipFile(self.zip_path, 'r') as z:
            csv_files = [f for f in z.namelist() if f.lower().startswith(self.prefix) and f.lower().endswith('.csv')]
            """
            if needed use prefix file 
            
            
            if f.lower().startswith(self.prefix) and f.lower().endswith('.csv')]
            """
            if not csv_files:
                raise ValueError(f"Non find '{self.prefix}' in ZIP ")
            self.csv_filename = csv_files[0]

    def read_csv_head(self, n=10):
        with zipfile.ZipFile(self.zip_path, 'r') as z:
            with z.open(self.csv_filename) as f:
                print(f"Open: {self.csv_filename}")
                df_head = pd.read_csv(f, nrows=n)
                return df_head

    def get_schema_from_sample(self, sample_size=100):
        with zipfile.ZipFile(self.zip_path, 'r') as z:
            with z.open(self.csv_filename) as f:
                df_sample = pd.read_csv(f, nrows=sample_size)
                schema = {col: str(dtype) for col, dtype in df_sample.dtypes.items()}
                return schema

    def read_csv_in_chunks(self, chunk_size=100_000, dtype_map=None, date_cols=None):
        with zipfile.ZipFile(self.zip_path, 'r') as z:
            with z.open(self.csv_filename) as f:
                chunk_iter = pd.read_csv(
                    f,
                    chunksize=chunk_size,
                    dtype=dtype_map,
                    parse_dates=date_cols,
                    low_memory=False
                )
                for i, chunk in enumerate(chunk_iter):
                    print(f"Chank {i + 1}: {len(chunk)} len")
                    yield chunk
    #Normalize name for db                
    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
      cols = df.columns.str.lower().str.replace(r'\s+', '_', regex=True)
      def remove_parentheses(s):
          return re.sub(r'[\(\)]', '', s)
      cols = cols.map(remove_parentheses)
      return df.rename(columns=dict(zip(df.columns, cols)))