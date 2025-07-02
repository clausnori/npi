import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import numpy as np
from typing import Dict, List, Any, Optional
import logging
import json
import os
from pathlib import Path

class NPI_Mongo:
    def __init__(self, connection_string: str, database_name: str, collection_name: str = "npi_providers", 
                 cache_dir: str = "cache"):
        """
        Args:
            connection_string: Atlas MongoDB
            database_name: name DB 
            collection_name: Collection name (default "npi_providers")
            cache_dir: Catha JSON(for taxonomy and identifier) dir
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        
        # Index for Npi number 
        self.collection.create_index("number", unique=True)
        
        # Catha Json dir Init if not find
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        self.taxonomy_cache_file = self.cache_dir / "taxonomy_codes.json"
        self.identifier_cache_file = self.cache_dir / "identifier_types.json"
        
        # Загружаем или создаем кэш
        self.taxonomy_descriptions = self._load_taxonomy_cache()
        self.identifier_types = self._load_identifier_cache()
        
    
    def _load_taxonomy_cache(self) -> Dict[str, str]:
        if self.taxonomy_cache_file.exists():
            try:
                with open(self.taxonomy_cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                self.logger.info(f"Loaded {len(cache)} taxonomy codes from cache")
                return cache
            except Exception as e:
                self.logger.warning(f"Failed to load taxonomy cache: {e}")
        
        # Basic taxonomy
        default_taxonomies = {
            "207X00000X": "Orthopaedic Surgery",
            "208D00000X": "General Practice",
            "207Q00000X": "Family Medicine",
            "207R00000X": "Internal Medicine",
            "208000000X": "Pediatrics",
            "207V00000X": "Obstetrics & Gynecology",
            "207T00000X": "Neurological Surgery",
            "208600000X": "Surgery",
            "207Y00000X": "Otolaryngology",
            "208M00000X": "Hospitalist",
            "364S00000X": "Clinical Nurse Specialist",
            "363L00000X": "Nurse Practitioner",
            "367500000X": "Nurse Anesthetist, Certified Registered",
            "374700000X": "Acupuncturist",
            "225100000X": "Physical Therapist",
            "103T00000X": "Psychologist",
            "261QP2300X": "Primary Care Clinic/Center",
            "282N00000X": "General Acute Care Hospital"
        }
        
        self._save_taxonomy_cache(default_taxonomies)
        return default_taxonomies
    
    def _load_identifier_cache(self) -> Dict[str, Dict]:
        if self.identifier_cache_file.exists():
            try:
                with open(self.identifier_cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                self.logger.info(f"Loaded {len(cache)} identifier types from cache")
                return cache
            except Exception as e:
                self.logger.warning(f"Failed to load identifier cache: {e}")
        
        # Basic identifier type
        default_identifiers = {
            "1": {"code": "01", "desc": "Other (non-Medicare)"},
            "2": {"code": "02", "desc": "Medicare UPIN"},
            "3": {"code": "03", "desc": "Medicare NSC"},
            "4": {"code": "04", "desc": "Medicare PIN"},
            "5": {"code": "05", "desc": "MEDICAID"},
            "6": {"code": "06", "desc": "Medicare OSCAR"},
            "7": {"code": "07", "desc": "TRICARE"},
            "8": {"code": "08", "desc": "Blue Cross Blue Shield"},
            "1.0": {"code": "01", "desc": "Other (non-Medicare)"},
            "2.0": {"code": "02", "desc": "Medicare UPIN"},
            "3.0": {"code": "03", "desc": "Medicare NSC"},
            "4.0": {"code": "04", "desc": "Medicare PIN"},
            "5.0": {"code": "05", "desc": "MEDICAID"},
            "6.0": {"code": "06", "desc": "Medicare OSCAR"}
        }
        
        self._save_identifier_cache(default_identifiers)
        return default_identifiers
    
    def _save_taxonomy_cache(self, taxonomy_dict: Dict[str, str]):
        try:
            with open(self.taxonomy_cache_file, 'w', encoding='utf-8') as f:
                json.dump(taxonomy_dict, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(taxonomy_dict)} taxonomy codes to cache")
        except Exception as e:
            self.logger.error(f"Failed to save taxonomy cache: {e}")
    
    def _save_identifier_cache(self, identifier_dict: Dict[str, Dict]):
        try:
            with open(self.identifier_cache_file, 'w', encoding='utf-8') as f:
                json.dump(identifier_dict, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(identifier_dict)} identifier types to cache")
        except Exception as e:
            self.logger.error(f"Failed to save identifier cache: {e}")
    
    def _create_unique_index(self):
      """Создает уникальный индекс по полю number (NPI)"""
      try:
          self.collection.create_index("number", unique=True)
          self.logger.info("Unique index created on 'number' field")
      except Exception as e:
          self.logger.error(f"Error creating unique index: {str(e)}")
    
    def _extract_taxonomies_from_dataframe(self, df: pd.DataFrame) -> Dict[str, str]:
        new_taxonomies = {}
        
        for i in range(1, 16):  # for 15 taxonomies 
            code_col = f'healthcare_provider_taxonomy_code_{i}'
            
            if code_col in df.columns:
                unique_codes = df[code_col].dropna().unique()
                for code in unique_codes:
                    code_str = str(code).strip()
                    if code_str and code_str not in self.taxonomy_descriptions:
                        # If description non find use code like ind..
                        new_taxonomies[code_str] = f"Taxonomy Code {code_str}"
        
        return new_taxonomies
    
    def _extract_identifiers_from_dataframe(self, df: pd.DataFrame) -> Dict[str, Dict]:
        new_identifiers = {}
        
        for i in range(1, 51):  # for 50 ind identifiers
            type_col = f'other_provider_identifier_type_code_{i}'
            
            if type_col in df.columns:
                unique_types = df[type_col].dropna().unique()
                for type_code in unique_types:
                    type_str = str(type_code).strip()
                    if type_str and type_str not in self.identifier_types:
                        # Standard structure for identifiers
                        code_num = type_str.replace('.0', '') if '.0' in type_str else type_str
                        new_identifiers[type_str] = {
                            "code": f"{int(float(code_num)):02d}" if code_num.isdigit() else "01",
                            "desc": f"Identifier Type {type_str}"
                        }
        
        return new_identifiers
    
    def update_caches_from_dataframe(self, df: pd.DataFrame) -> Dict[str, int]:
      
        new_taxonomies = self._extract_taxonomies_from_dataframe(df)
        new_identifiers = self._extract_identifiers_from_dataframe(df)
        
        # Update Catha if need 
        taxonomies_added = 0
        if new_taxonomies:
            self.taxonomy_descriptions.update(new_taxonomies)
            self._save_taxonomy_cache(self.taxonomy_descriptions)
            taxonomies_added = len(new_taxonomies)
            self.logger.info(f"Added {taxonomies_added} new taxonomy codes")
        
        identifiers_added = 0
        if new_identifiers:
            self.identifier_types.update(new_identifiers)
            self._save_identifier_cache(self.identifier_types)
            identifiers_added = len(new_identifiers)
            self.logger.info(f"Added {identifiers_added} new identifier types")
        
        return {
            "taxonomies_added": taxonomies_added,
            "identifiers_added": identifiers_added
        }
    
    def get_taxonomy_description(self, code: str) -> str:
        # description taxonomy from code Standard taxonomy
        return self.taxonomy_descriptions.get(str(code).strip(), f"Unknown Taxonomy ({code})")
    
    def get_identifier_type(self, type_code: str) -> Dict[str, str]:
        #Type identifier 
        return self.identifier_types.get(str(type_code).strip(), {"code": "01", "desc": f"Unknown Type ({type_code})"})
    
    def _convert_nan_to_none(self, value):
    # IMPORTANT : Convert Nan in None for MONGO
        if pd.isna(value):
            return None
        return value
    
    def _parse_date(self, date_str):
        #Parser for data NPI format MM/DD/YYYY in Python 
        if pd.isna(date_str) or date_str is None:
            return None
        try:
            return datetime.strptime(str(date_str), "%m/%d/%Y").isoformat()
        except:
            return str(date_str)
    
    def _extract_addresses(self, row: pd.Series) -> List[Dict]:
        addresses = []
        
        # Mailing address
        if not pd.isna(row.get('provider_first_line_business_mailing_address')):
            mailing_addr = {
                "country_code": self._convert_nan_to_none(row.get('provider_business_mailing_address_country_code_if_outside_u.s.')),
                "country_name": "United States" if row.get('provider_business_mailing_address_country_code_if_outside_u.s.') == 'US' else None,
                "address_purpose": "MAILING",
                "address_type": "DOM",
                "address_1": self._convert_nan_to_none(row.get('provider_first_line_business_mailing_address')),
                "address_2": self._convert_nan_to_none(row.get('provider_second_line_business_mailing_address')),
                "city": self._convert_nan_to_none(row.get('provider_business_mailing_address_city_name')),
                "state": self._convert_nan_to_none(row.get('provider_business_mailing_address_state_name')),
                "postal_code": str(int(row.get('provider_business_mailing_address_postal_code'))) if not pd.isna(row.get('provider_business_mailing_address_postal_code')) else None,
                "telephone_number": self._format_phone(row.get('provider_business_mailing_address_telephone_number')),
                "fax_number": self._format_phone(row.get('provider_business_mailing_address_fax_number'))
            }
            addresses.append(mailing_addr)
        
        # Practice location address
        if not pd.isna(row.get('provider_first_line_business_practice_location_address')):
            location_addr = {
                "country_code": self._convert_nan_to_none(row.get('provider_business_practice_location_address_country_code_if_outside_u.s.')),
                "country_name": "United States" if row.get('provider_business_practice_location_address_country_code_if_outside_u.s.') == 'US' else None,
                "address_purpose": "LOCATION",
                "address_type": "DOM",
                "address_1": self._convert_nan_to_none(row.get('provider_first_line_business_practice_location_address')),
                "address_2": self._convert_nan_to_none(row.get('provider_second_line_business_practice_location_address')),
                "city": self._convert_nan_to_none(row.get('provider_business_practice_location_address_city_name')),
                "state": self._convert_nan_to_none(row.get('provider_business_practice_location_address_state_name')),
                "postal_code": str(int(row.get('provider_business_practice_location_address_postal_code'))) if not pd.isna(row.get('provider_business_practice_location_address_postal_code')) else None,
                "telephone_number": self._format_phone(row.get('provider_business_practice_location_address_telephone_number')),
                "fax_number": self._format_phone(row.get('provider_business_practice_location_address_fax_number'))
            }
            addresses.append(location_addr)
        
        return addresses
    
    def _format_phone(self, phone):
        #
        if pd.isna(phone):
            return None
        phone_str = str(int(phone)) if isinstance(phone, float) else str(phone)
        if len(phone_str) == 10:
            return f"{phone_str[:3]}-{phone_str[3:6]}-{phone_str[6:]}"
        return phone_str
    
    def _extract_taxonomies(self, row: pd.Series) -> List[Dict]:
        taxonomies = []
        
        for i in range(1, 16):  # До 15 таксономий
            code_col = f'healthcare_provider_taxonomy_code_{i}'
            license_col = f'provider_license_number_{i}'
            state_col = f'provider_license_number_state_code_{i}'
            primary_col = f'healthcare_provider_primary_taxonomy_switch_{i}'
            group_col = f'healthcare_provider_taxonomy_group_{i}'
            
            if not pd.isna(row.get(code_col)):
                code = str(row.get(code_col)).strip()
                taxonomy = {
                    "code": code,
                    "taxonomy_group": self._convert_nan_to_none(row.get(group_col)),
                    "desc": self.get_taxonomy_description(code),
                    "state": self._convert_nan_to_none(row.get(state_col)),
                    "license": self._convert_nan_to_none(row.get(license_col)),
                    "primary": row.get(primary_col) == 'Y'
                }
                taxonomies.append(taxonomy)
        
        return taxonomies
    
    def _extract_identifiers(self, row: pd.Series) -> List[Dict]:
        identifiers = []
        
        for i in range(1, 51):  # 50 identifiers
            id_col = f'other_provider_identifier_{i}'
            type_col = f'other_provider_identifier_type_code_{i}'
            state_col = f'other_provider_identifier_state_{i}'
            issuer_col = f'other_provider_identifier_issuer_{i}'
            
            if not pd.isna(row.get(id_col)):
                type_code = str(row.get(type_col)) if not pd.isna(row.get(type_col)) else "1"
                type_info = self.get_identifier_type(type_code)
                
                identifier = {
                    "code": type_info["code"],
                    "desc": type_info["desc"],
                    "issuer": self._convert_nan_to_none(row.get(issuer_col)),
                    "identifier": self._convert_nan_to_none(row.get(id_col)),
                    "state": self._convert_nan_to_none(row.get(state_col))
                }
                identifiers.append(identifier)
        
        return identifiers
    
    def _extract_basic_info(self, row: pd.Series) -> Dict:
        #Basic Providers Data 
        return {
            "first_name": self._convert_nan_to_none(row.get('provider_first_name')),
            "last_name": self._convert_nan_to_none(row.get('provider_last_name_legal_name')),
            "middle_name": self._convert_nan_to_none(row.get('provider_middle_name')),
            "credential": self._convert_nan_to_none(row.get('provider_credential_text')),
            "sole_proprietor": "YES" if row.get('is_sole_proprietor') == 'Y' else "NO",
            "sex": self._convert_nan_to_none(row.get('provider_sex_code')),
            "enumeration_date": self._parse_date(row.get('provider_enumeration_date')),
            "last_updated": self._parse_date(row.get('last_update_date')),
            "status": "A" if pd.isna(row.get('npi_deactivation_date')) else "D",
            "name_prefix": self._convert_nan_to_none(row.get('provider_name_prefix_text')) or "--",
            "name_suffix": self._convert_nan_to_none(row.get('provider_name_suffix_text')) or "--",
            "organization_name": self._convert_nan_to_none(row.get('provider_organization_name_legal_business_name'))
        }
    
    def _convert_dataframe_row_to_document(self, row: pd.Series) -> Dict:
        """Конвертирует строку DataFrame в документ MongoDB в формате API"""
        
        # Определяем тип перечисления
        entity_type = row.get('entity_type_code')
        enumeration_type = "NPI-1" if entity_type == 1.0 else "NPI-2"
        
        # Конвертируем даты в epoch
        enum_date = self._parse_date(row.get('provider_enumeration_date'))
        update_date = self._parse_date(row.get('last_update_date'))
        
        created_epoch = None
        updated_epoch = None
        
        if enum_date:
            try:
                created_epoch = str(int(datetime.fromisoformat(enum_date.replace('Z', '')).timestamp() * 1000))
            except:
                pass
                
        if update_date:
            try:
                updated_epoch = str(int(datetime.fromisoformat(update_date.replace('Z', '')).timestamp() * 1000))
            except:
                pass
        
        document = {
            "created_epoch": created_epoch,
            "enumeration_type": enumeration_type,
            "last_updated_epoch": updated_epoch,
            "number": str(int(row.get('npi'))) if not pd.isna(row.get('npi')) else None,
            "addresses": self._extract_addresses(row),
            "practiceLocations": [],#TODO
            "basic": self._extract_basic_info(row),
            "taxonomies": self._extract_taxonomies(row),
            "identifiers": self._extract_identifiers(row),
            "endpoints": [],  # TODO
            "other_names": []  # TODO
        }
        
        return document
    
    def insert(self, df: pd.DataFrame, update_cache: bool = True) -> Dict[str, Any]:
        try:
            #If need we update Caches
            cache_update_result = {"taxonomies_added": 0, "identifiers_added": 0}
            if update_cache:
                cache_update_result = self.update_caches_from_dataframe(df)
            
            documents = []
            errors = []
            
            for index, row in df.iterrows():
                try:
                    document = self._convert_dataframe_row_to_document(row)
                    if document["number"]:  # Verification,for NPI Number 
                        documents.append(document)
                    else:
                        errors.append(f"Row {index}: Missing NPI number")
                except Exception as e:
                    errors.append(f"Row {index}: {str(e)}")
            
            if documents:
                # Use insert_many с ordered=False if mistake happen 
                try:
                    result = self.collection.insert_many(documents, ordered=False)
                    inserted_count = len(result.inserted_ids)
                except Exception as e:
                    # If have more 
                    inserted_count = 0
                    for doc in documents:
                        try:
                            self.collection.insert_one(doc)
                            inserted_count += 1
                        except Exception as insert_error:
                            errors.append(f"NPI {doc.get('number')}: {str(insert_error)}")
            else:
                inserted_count = 0
            
            result = {
                "success": True,
                "inserted_count": inserted_count,
                "total_processed": len(df),
                "errors": errors,
                "cache_updates": cache_update_result
            }
            
            self.logger.info(f"Inserted {inserted_count} documents out of {len(df)} processed")
            if cache_update_result["taxonomies_added"] > 0 or cache_update_result["identifiers_added"] > 0:
                self.logger.info(f"Cache updated: {cache_update_result['taxonomies_added']} taxonomies, {cache_update_result['identifiers_added']} identifiers")
            if errors:
                self.logger.warning(f"Encountered {len(errors)} errors during insertion")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during insertion: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "inserted_count": 0,
                "total_processed": len(df),
                "errors": [str(e)],
                "cache_updates": {"taxonomies_added": 0, "identifiers_added": 0}
            }
    
    def update(self, df: pd.DataFrame, update_cache: bool = True) -> Dict[str, Any]:
        try:
            # update_cache
            cache_update_result = {"taxonomies_added": 0, "identifiers_added": 0}
            if update_cache:
                cache_update_result = self.update_caches_from_dataframe(df)
            
            updated_count = 0
            not_found_count = 0
            errors = []
            
            for index, row in df.iterrows():
                try:
                    document = self._convert_dataframe_row_to_document(row)
                    npi_number = document.get("number")
                    
                    if not npi_number:
                        errors.append(f"Row {index}: Missing NPI number")
                        continue
                    
                    result = self.collection.replace_one(
                        {"number": npi_number},
                        document,
                        upsert=False
                    )
                    
                    if result.matched_count > 0:
                        updated_count += 1
                    else:
                        not_found_count += 1
                        
                except Exception as e:
                    errors.append(f"Row {index}: {str(e)}")
            
            result = {
                "success": True,
                "updated_count": updated_count,
                "not_found_count": not_found_count,
                "total_processed": len(df),
                "errors": errors,
                "cache_updates": cache_update_result
            }
            
            self.logger.info(f"Updated {updated_count} documents, {not_found_count} not found out of {len(df)} processed")
            if cache_update_result["taxonomies_added"] > 0 or cache_update_result["identifiers_added"] > 0:
                self.logger.info(f"Cache updated: {cache_update_result['taxonomies_added']} taxonomies, {cache_update_result['identifiers_added']} identifiers")
            if errors:
                self.logger.warning(f"Encountered {len(errors)} errors during update")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during update: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "updated_count": 0,
                "total_processed": len(df),
                "errors": [str(e)],
                "cache_updates": {"taxonomies_added": 0, "identifiers_added": 0}
            }
    
    def find_by_npi(self, npi: str) -> Optional[Dict]:
        return self.collection.find_one({"number": str(npi)})
        
    def get_all_as_dict(self) -> Dict[str, Dict]:
      result = {}
      for doc in self.collection.find():
          npi = str(doc.get("number"))
          result[npi] = doc
      return result
      
    def export_caches(self, export_dir: str = "exported_caches"):
        export_path = Path(export_dir)
        export_path.mkdir(exist_ok=True)
      
        taxonomy_export = export_path / "taxonomy_codes_export.json"
        with open(taxonomy_export, 'w', encoding='utf-8') as f:
            json.dump(self.taxonomy_descriptions, f, indent=2, ensure_ascii=False)
        
        identifier_export = export_path / "identifier_types_export.json"
        with open(identifier_export, 'w', encoding='utf-8') as f:
            json.dump(self.identifier_types, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Caches exported to {export_dir}")
    
    def get_cache_stats(self) -> Dict[str, int]:
        #Stat Catha for debug 
        return {
            "taxonomy_codes_count": len(self.taxonomy_descriptions),
            "identifier_types_count": len(self.identifier_types)
        }
    
    def close(self):
        self.client.close()