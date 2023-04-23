#!/usr/bin/env python3
from mo_sql_parsing import parse
import copy
import re

class InsertParser():

    def __init__(self):
        pass

    @staticmethod
    def conversion(proto, string_to_parse):
        
        fields = string_to_parse.split(",")
        
        for i,k in enumerate(proto.keys()):
            type_form = type(proto[k])
            proto[k] = type_form(fields[i])
    
        return copy.copy(proto)    

    def parse(self, query):
        try:
            split_statement = query.split(',(', 1)
            statement_subset = split_statement[0]+ ",(" +split_statement[1].split("),")[0] + ")" + ";"
        except:
            split_statement = query.split(', (', 1)
            statement_subset = split_statement[0]+ ", (" +split_statement[1].split("),")[0] + ")" + ";"

        full_table = "("+split_statement[1]
        
        regex = re.compile("\(.+?\)")
        itr = regex.findall(full_table)
        
        x = parse(statement_subset)
        prototype = copy.copy(x['values'][0])
        
        for match in itr[1:]:
            obj = match[1:-1]
            to_insert = InsertParser.conversion(prototype, obj)
            x["values"].append(to_insert)
        
        return x



if __name__ == "__main__":

    query = "INSERT INTO relii10 (z, y) VALUES (1, 1),(2, 2),(3, 3),(4, 4),(5, 5),(6, 6),(7, 7),(8, 8),(9, 9),(10, 10);"

    parser = InsertParser()

    with open("demo_tables.sql", "r") as in_f:
        for line in in_f:
            print(line)
            if line[:6] == "INSERT":
                print("Parsing statement...")
                x = parser.parse(line)
                print(x)
                print("Done...")
    #parser = InsertParser()
    #parsed = parser.parse(query)    
