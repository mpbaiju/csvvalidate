import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation, MatchesPatternValidation
import numpy as np
import re
import warnings
import time
import sys
import csv
import json
import logging
import os
from enum import Enum

warnings.filterwarnings("ignore", 'This pattern has match groups')
warnings.simplefilter(action='ignore', category=FutureWarning)

class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
        
class ErrorType(ExtendedEnum):
    NON_STRING_IN_HEADER_ERROR = 'Type1:Non-StringInHeaderError'
    DUPLICATE_HEADER_ERROR = 'Type2:DuplicateHeaderError'
    NO_TIMESTAMP_ERROR = 'Type3:NoTimeStampColumnError' 
    COLUMN_COUNT_ERROR = 'Type4:ColumnCountError'  
    NULL_DATA_ERROR = 'Type5:NullDataError'
    SCHEMA_ERROR = 'Type6:SchemaError' 

## Usage: CSVValidate(file_path, [loggerlevel])
class CSVValidate:
    string_pattern = "^[\w]+[\w \.]*$"
    timestamp_patterns = [
        ("^\d{2}\-\d{2}\-\d{4} \d{2}\:\d{2}\:\d{2}(\.\d+)?$", "timestamp"),
        ("^\d{4}\-\d{2}\-\d{2} \d{2}\:\d{2}\:\d{2}(\.\d+)?$", "timestamp"),
        ("^\d{2}\:\d{2}\:\d{2}(\.\d+)?$", "timestamp"),
        ("^\d{2}\-\d{2}\-\d{4}$", "timestamp"),
        ("^\d{4}\-\d{2}\-\d{2}$", "timestamp")
    ]
    
    valid_patterns = timestamp_patterns + [
        ("^\d+(\.\d+)$", "float"),
        ("^\d{10}$", "phone"),
        ("^\d+$", "integer"),
        ("^[\w\d]+$", "alphanumeric"),
        (string_pattern, "string"),
        ("^.*$", "text")
    ]
    check_for_column_counts = False
    datacolumns = [] 
    column_data_types_json = []
    error_json = [] 
        
    def __init__(self, filename, loggerlevel="DEBUG"):
        self.filename = filename  
        
        # Create a custom logger
        logging.basicConfig(level = loggerlevel)
        self.logger = logging.getLogger()

        # Create handlers
        c_handler = logging.StreamHandler()

        # Create formatters and add it to handlers
        c_format = logging.Formatter(
            '%(asctime)s : Line No. : %(name)s %(lineno)d  - %(levelname)s - %(message)s')
        c_handler.setFormatter(c_format)

        # Add handlers to the logger
        self.logger.addHandler(c_handler)
    
    def header_check(self):
        
        # read the data
        data = pd.read_csv(self.filename, sep=',', header=None, nrows=1, keep_default_na=False) 
                     
        datacolumns = []             
        for index, column in enumerate(data):
            fieldname = data[column][0] if len(str(data[column][0]).strip()) else f"Col{index}"
                        
            # check for duplicates in column names
            if fieldname in datacolumns:
                self.error_json.append({"Type": ErrorType.DUPLICATE_HEADER_ERROR.value, 
                    "Location" : {'column' : f"\"{fieldname}\""}, "Info" : "Duplicate value"})
                self.datacolumns.append(f"{fieldname}-Col{index}")
            else:
                newname = fieldname if len(str(data[column][0])) else f"Col{index}"
                datacolumns.append(newname) 
                self.datacolumns.append(newname) 
                                    
            # check for strings in column titles
            self.logger.debug(f"Pettern = {self.string_pattern}, Fieldname = {fieldname}")
            result = re.match(self.string_pattern, str(fieldname))
            if not result:
                self.logger.error(f"found non string header {fieldname}")
                self.error_json.append({"Type": ErrorType.NON_STRING_IN_HEADER_ERROR.value, 
                    "Location" : {'column' : fieldname}, 
                    "Info" : "column header is not of string type"})
            
        self.logger.debug(f"data columns = {self.datacolumns}")
        
    def  check_column_count(self, col_count):
        with open(self.filename, newline='') as f:
            reader = csv.reader(f)
            for index, row in enumerate(reader):
                actual_count = len(row)
                if actual_count != col_count:
                    self.error_json.append({"Type": ErrorType.COLUMN_COUNT_ERROR.value, 
                        "Location" : {'row' : index}, 
                        "Info" : f"Actual column count {actual_count} is not matching "
                            f"with column count of table {col_count}"})
            
    def validate_data(self):
        try:
            self.data = pd.read_csv(self.filename, sep=',', header=None, skiprows = 1)
        except:
            self.data = pd.read_csv(self.filename, sep=',', error_bad_lines=False, 
                header=None, skiprows = 1)  
            self.check_for_column_counts = True
            
        null_validation = [CustomElementValidation(lambda d: d is not np.nan, 
            'this field cannot be null')]
        schema_validators = []   
        self.data.columns = self.datacolumns  
        found_timestamp = False 
        for index, column in enumerate(self.data):
            field = ''
            for cellval in self.data[column]:
                if not pd.isna(cellval) and len(str(cellval).strip()):
                    field = str(cellval)
                    self.logger.debug(f"found first valid value {field} for column {index}")
                    break
                    
            if not len(field):
                self.logger.error("Couldn't find valid param for column {index}")
            for pattern in self.valid_patterns:
                result = re.match(pattern[0], field)
                if result:
                    self.logger.debug(f"found a match with {pattern[0]}")
                    schema_validators.append(Column(self.datacolumns[index], 
                        [MatchesPatternValidation(pattern[0])] + null_validation)) 
                    self.column_data_types_json.append({"Column" : str(self.datacolumns[index]), 
                        "type" : pattern[1], "pattern" : pattern[0]})
                    self.logger.debug(f"pattern = {pattern}")
                    if pattern[1] == "timestamp" :
                        found_timestamp = True
                    break        
                    
        # check for timestamp field
        if not found_timestamp:
            self.logger.error("No timestamp field..")
            self.error_json.append({"Type": ErrorType.NO_TIMESTAMP_ERROR.value, 
                "Location" : {}, "Info" : "could not find any timestamp column"})
        
        schema = pandas_schema.Schema(schema_validators)
        
        # apply validation
        errors = schema.validate(self.data)
        for error1 in errors:
            details = str(error1).split(': ',3)
            self.error_json.append({"Type": ErrorType.SCHEMA_ERROR.value, 
                "Location" : {"row" : details[1].split(',')[0], 
                "column" : details[2][0:-1]}, "Info": details[3]})       
       
        if self.check_for_column_counts:
            self.check_column_count(len(self.datacolumns))
         
        basefilename = os.path.splitext(self.filename)[0]
        error_json_filename = basefilename + "_errors.json"
        datatype_json_filename = basefilename + "_datatypes.json"
        self.logger.debug(f"data types = {self.column_data_types_json}")
        with open(datatype_json_filename, 'w') as f:
            json.dump(self.column_data_types_json, f, ensure_ascii=False, indent=4)
        with open(error_json_filename, 'w') as f:
            json.dump(self.error_json, f, ensure_ascii=False, indent=4)
    
    def validate(self):
        self.header_check()
        self.validate_data()

if __name__ == '__main__':
    start_time = time.time()
    csvvalidate = CSVValidate(sys.argv[1])
    # csvvalidate = CSVValidate(sys.argv[1], "INFO")
    csvvalidate.validate()
    csvvalidate.logger.debug(f"%s seconds for {sys.argv[1]}" % (time.time() - start_time))