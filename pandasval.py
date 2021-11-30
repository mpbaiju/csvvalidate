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

warnings.filterwarnings("ignore", 'This pattern has match groups')
        
valid_patterns = [
        "^\d{2}\-\d{2}\-\d{4}$",
        "^\d{4}\-\d{2}\-\d{2}$",
        "^\d+(\.\d+)$",
        "^\d{10}$",
        "^\d+$",
        "^[\w\d]+$",
        "^.*$"
    ]

def check_column_count(filename, col_count): 
    errors = [] 
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        for index, row in enumerate(reader):
            actual_count = len(row)
            if actual_count != col_count:
                errors.append(f'"{{row: {index}}}": actual column count {actual_count} is not matching with column count of table {col_count}')
    return errors

def do_validation(filename):
    # read the data
    check_for_column_counts = False
    try:
        data = pd.read_csv(filename, sep=',', header=None)
    except:
        data = pd.read_csv(filename, sep=',', header=None, error_bad_lines=False)  
        check_for_column_counts = True
    
    schema_validators = []
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]
    
    datacolumns = []
    for index, column in enumerate(data):
        datacolumns.append(f'cell{index}')
    data.columns = datacolumns
    
    for index, column in enumerate(data):
        field = ''
        for cellval in data[column][1:]:
            if not pd.isna(cellval) and len(str(cellval).strip()):
                field = str(cellval)
                print(f"found first valid value {field} for column {index}")
                break
                
        if not len(field):
            print("Couldn't find valid param for column {index}")
        for pattern in valid_patterns:
            result = re.match(pattern, field)
            if result:
                print(f"found a match with {pattern}")
                schema_validators.append(Column(datacolumns[index], [MatchesPatternValidation(pattern)] + null_validation)) 
                break        
     
    schema = pandas_schema.Schema(schema_validators)
    
    # apply validation
    errors = schema.validate(data)

    # save data
    pd.DataFrame({'col':errors}).to_csv('errors.csv')
    if check_for_column_counts:
        error_count = len(errors)
        print(f"error count = {error_count}")
        column_count_errors = check_column_count(filename, len(datacolumns))
        file1 = open("errors.csv", "a")
        for row in column_count_errors:
            file1.write(f"{error_count},{row}\n")
            error_count += 1
        file1.close()

if __name__ == '__main__':
    start_time = time.time()
    do_validation(sys.argv[1])
    print(f"%s seconds for {sys.argv[1]}" % (time.time() - start_time))