import re
import zipfile
import pandas as pd
from pandas import DataFrame
from typing import Optional, Dict, List, Generator, Any
import os


class NPI_Load:
    def __init__(self, file_path: str, prefix: str = "", csv_filename: Optional[str] = None):
        """
        Initialization of the class for working with NPI data in a CSV or ZIP file

Args:

file_path (str): Path to the CSV or ZIP file

prefix (str): Prefix used to search for CSV files in the ZIP (ignored for standalone CSV)

csv_filename (str, optional): Specific name of the CSV file inside the ZIP (if known)
        """
        self.file_path = file_path
        self.prefix = prefix.lower() if prefix else ""
        self.csv_filename = csv_filename
        self.is_zip = file_path.lower().endswith('.zip')
        
        # Valide file in init
        self._validate_file()
        
        if self.is_zip and not self.csv_filename:
            self._find_csv_file()
    
    def _validate_file(self) -> None:
        if not os.path.exists(self.file_path):
            raise ValueError(f"File not find: {self.file_path}")
        
        if self.is_zip:
            try:
                with zipfile.ZipFile(self.file_path, 'r') as z:
                    z.testzip()
            except (zipfile.BadZipFile, FileNotFoundError) as e:
                raise ValueError(f"Non correct Zip: {self.file_path}. Exp: {e}")
        else:
            if not self.file_path.lower().endswith('.csv'):
                raise ValueError(f"Only for CVS or Zip: {self.file_path}")
    
    def _find_csv_file(self) -> None:
        with zipfile.ZipFile(self.file_path, 'r') as z:
            csv_files = [
                f for f in z.namelist() 
                if f.lower().endswith('.csv') and f.lower().startswith(self.prefix)
            ]
            
            if not csv_files:
                #if not find prefix , we just pick first file
                all_csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                if all_csv_files:
                    self.csv_filename = all_csv_files[0]
                    print(f"File which '{self.prefix}' Non find. Use: {self.csv_filename}")
                else:
                    raise ValueError(f"CSV Not find in Zip \n Set file_patch in method: {self.file_path}")
            else:
                self.csv_filename = csv_files[0]
                print(f"CVS File Find: {self.csv_filename}")
    
    def _get_file_handle(self):
        if self.is_zip:
            zip_file = zipfile.ZipFile(self.file_path, 'r')
            csv_file = zip_file.open(self.csv_filename)
            return zip_file, csv_file
        else:
            return None, open(self.file_path, 'r', encoding='utf-8')
    
    def read_csv_head(self, n: int = 10) -> pd.DataFrame:
        """DEV method for check file structure 
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Read {n} in : {filename}")
            df_head = pd.read_csv(csv_file, nrows=n)
            return df_head
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def get_schema_from_sample(self, sample_size: int = 100) -> Dict[str, str]:
        """
        Get fillds in CSV 
        
        Args:
            sample_size (int): Size for analysis 
            
        Returns:
            Dict[str, str]: Type and Name
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            df_sample = pd.read_csv(csv_file, nrows=sample_size)
            schema = {col: str(dtype) for col, dtype in df_sample.dtypes.items()}
            return schema
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def read_csv_in_chunks(self, 
                          chunk_size: int = 100_000, 
                          dtype_map: Optional[Dict[str, Any]] = None, 
                          date_cols: Optional[List[str]] = None) -> Generator[pd.DataFrame, None, None]:
        """
        chunks CSV
        
        Args:
            chunk_size (int): Size one part
            dtype_map (Dict, optional): Field type
            date_cols (List[str], optional): List collons for parse
            
        Yields:
            pd.DataFrame: Part Data
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False
            )
            
            for i, chunk in enumerate(chunk_iter):
                print(f"Chunk {i + 1}: {len(chunk)} len")
                yield chunk
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def read_full_csv(self, 
                     dtype_map: Optional[Dict[str, Any]] = None, 
                     date_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Чтение всего CSV файла целиком
        
        Args:
            dtype_map (Dict, optional): Словарь типов данных для колонок
            date_cols (List[str], optional): Список колонок для парсинга дат
            
        Returns:
            pd.DataFrame: Полный DataFrame
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Чтение полного файла: {filename}")
            df = pd.read_csv(
                csv_file,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False
            )
            return df
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Нормализация имен колонок для БД
        
        Args:
            df (pd.DataFrame): Исходный DataFrame
            
        Returns:
            pd.DataFrame: DataFrame с нормализованными именами колонок
        """
        # snake_case
        cols = df.columns.str.lower().str.replace(r'\s+', '_', regex=True)
        
        # del ( & )
        cols = cols.str.replace(r'[\(\)]', '', regex=True)
        
        cols = cols.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        
        # Multiple _ 
        cols = cols.str.replace(r'_+', '_', regex=True)
        
        # Del _ in start and end
        cols = cols.str.strip('_')
        
        return df.rename(columns=dict(zip(df.columns, cols)))
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Get CVS Data information 
        
        Returns:
            Dict: Data information 
        """
        info = {
            'file_path': self.file_path,
            'is_zip': self.is_zip,
            'file_size': os.path.getsize(self.file_path)
        }
        
        if self.is_zip:
            info['csv_filename'] = self.csv_filename
            info['prefix'] = self.prefix
            
            with zipfile.ZipFile(self.file_path, 'r') as z:
                zip_info = z.getinfo(self.csv_filename)
                info.update({
                    'file_size_compressed': zip_info.compress_size,
                    'file_size_uncompressed': zip_info.file_size,
                    'compression_type': zip_info.compress_type,
                    'date_time': zip_info.date_time
                })
        else:
            info['csv_filename'] = os.path.basename(self.file_path)
        
        return info