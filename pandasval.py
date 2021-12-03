import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation, MatchesPatternValidation
import numpy as np
import re
from decimal import *
import warnings
import time
import sys
import fileinput
import csv
import json

warnings.filterwarnings("ignore", 'This pattern has match groups')

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
    errors = []
    check_for_column_counts = False
    datacolumns = [] 
    column_data_types = []
    
    def __init__(self, filename):
        self.filename = filename        
    
    def header_check(self):
        
        # read the data
        try:
            data = pd.read_csv(self.filename, sep=',', header=None)
        except:
            data = pd.read_csv(self.filename, sep=',', header=None, error_bad_lines=False) 
            self.check_for_column_counts = True
                     
        datacolumns = []             
        for index, column in enumerate(data):
            fieldname = str(data[column][0])
                        
            # check for duplicates in column names
            if fieldname in datacolumns:
                self.errors.append(f'"{{column: {fieldname}}}": duplicate value')
            else:
                datacolumns.append(fieldname)            
            self.datacolumns.append(f"Col{index}:{fieldname}") 
            
            # check for strings in column titles
            result = re.match(self.string_pattern, fieldname)
            if not result:
                print(f"found non string header {fieldname}")
                self.errors.append(f'"{{column: {fieldname}}}": is not of string type')  
        try:
            self.data = pd.read_csv(self.filename, sep=',', header=None, skiprows = 1)
        except:
            self.data = pd.read_csv(self.filename, sep=',', error_bad_lines=False, header=None, skiprows = 1)  
                  
    def  check_column_count(self, col_count):
        with open(self.filename, newline='') as f:
            reader = csv.reader(f)
            for index, row in enumerate(reader):
                actual_count = len(row)
                if actual_count != col_count:
                    self.errors.append(f'"{{row: {index}}}": actual column count {actual_count} is not matching with column count of table {col_count}')
            
    def validate_data(self):
        null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]
        schema_validators = []   
        self.data.columns = self.datacolumns  
        found_timestamp = False 
        for index, column in enumerate(self.data):
            field = ''
            for cellval in self.data[column]:
                if not pd.isna(cellval) and len(str(cellval).strip()):
                    field = str(cellval)
                    print(f"found first valid value {field} for column {index}")
                    break
                    
            if not len(field):
                print("Couldn't find valid param for column {index}")
            for pattern in self.valid_patterns:
                result = re.match(pattern[0], field)
                if result:
                    print(f"found a match with {pattern[0]}")
                    schema_validators.append(Column(self.datacolumns[index], [MatchesPatternValidation(pattern[0])] + null_validation)) 
                    self.column_data_types.append({"Column" : self.datacolumns[index], "type" : pattern[1], "pattern" : pattern[0]})
                    print(f"pattern = {pattern}")
                    if pattern[1] == "timestamp" :
                        found_timestamp = True
                    break        
                    
        # check for timestamp field
        if not found_timestamp:
            print("No timestamp field..")
            self.errors.append(f"header : could not find any timestamp column")
        
        schema = pandas_schema.Schema(schema_validators)
        
        # apply validation
        errors = schema.validate(self.data)

        # save data
        pd.DataFrame({'col':errors}).to_csv('errors.csv')
        error_count = len(errors)
        print(f"error count = {error_count}")
        if self.check_for_column_counts:
            self.check_column_count(len(self.datacolumns))
            
        if len(self.errors):
            file1 = open("errors.csv", "a")
            for row in self.errors:
                file1.write(f"{error_count},{row}\n")
                error_count += 1
            file1.close()
         
        with open('datatypes.json', 'w') as f:
            json.dump(self.column_data_types, f, indent=4)
    
    def validate(self):
        self.header_check()
        self.validate_data()

if __name__ == '__main__':
    start_time = time.time()
    csvvalidate = CSVValidate(sys.argv[1])
    csvvalidate.validate()
    print(f"%s seconds for {sys.argv[1]}" % (time.time() - start_time))