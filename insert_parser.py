"""
from mo_sql_parsing import parse
import copy
import re


class InsertParser():

    def __init__(self):
        pass

    @staticmethod
    def conversion(proto, string_to_parse):

        fields = string_to_parse.split(",")

        for i, k in enumerate(proto.keys()):
            type_form = type(proto[k])
            proto[k] = type_form(fields[i])

        return copy.copy(proto)

    def parse(self, query):
        try:
            split_statement = query.split(',(', 1)
            statement_subset = split_statement[0] + ",(" + split_statement[1].split("),")[0] + ")" + ";"
        except:
            split_statement = query.split(', (', 1)
            statement_subset = split_statement[0] + ", (" + split_statement[1].split("),")[0] + ")" + ";"

        full_table = "(" + split_statement[1]

        regex = re.compile("\(.+?\)")
        itr = regex.findall(full_table)

        x = parse(statement_subset)
        prototype = copy.copy(x['values'][0])

        for match in itr[1:]:
            obj = match[1:-1]
            to_insert = InsertParser.conversion(prototype, obj)
            x["values"].append(to_insert)

        return x
"""

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
            if type_form == str:
                proto[k] = InsertParser.remove_quotes(type_form(fields[i]))
            else:
                proto[k] = type_form(fields[i])

        return copy.copy(proto)

    @staticmethod
    def remove_quotes(s):
        return s.strip().strip('"').strip("'")

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
        #print(x)
        prototype = copy.copy(x['values'][0])

        for match in itr[1:]:
            obj = match[1:-1]
            to_insert = InsertParser.conversion(prototype, obj)
            x["values"].append(to_insert)

        return x
