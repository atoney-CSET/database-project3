from BTrees.OOBTree import OOBTree
from output import Output
from operator import itemgetter
import pandas as pd
from tabulate import tabulate

class Table:
    def __init__(self, name, fields, pk, fk, ref, indexes):
        self.name = name
        self.rows = {}
        self.fields = fields
        self.primary_key = pk
        self.foreign_key = fk
        self.reference = ref
        self.indexes = indexes

    def handle(self, query):
        sql_type = query.get("Type")
        if sql_type == "INSERT":
            return self.insert(query)
        elif sql_type == "DELETE":
            return self.delete(query)
        elif sql_type == "UPDATE":
            return self.update(query)
        # elif sql_type == "ALTER TABLE":
        elif sql_type == "SELECT":
            return self.search(query)
        elif sql_type == "CREATE INDEX":
            self.create_index(query)
            return None
        elif sql_type == "DROP INDEX":
            self.drop_index(query)
            return None
        else:
            print("Error: SQL type " + sql_type + " is not supported.")
            return None

    def insert(self, query):

        rows = query.get("values")

        if self.primary_key is not None:
            for r in rows:

                pk_value = r.get(self.primary_key)
                if self.rows.get(pk_value) is not None:
                    print("<<<<<<Error: Duplicate record! Primary key exists.\n")
                    break
                field_value = []
                for field in r:
                    field_value.insert(self.fields.get(field).get("index"), r.get(field))

                # update table
                self.rows[pk_value] = field_value
                #print("Insert success!")
                # update index on primary key and on other fields
                self.update_index(pk_value, "insert")

            # for index in self.indexes:
            # print(list(self.indexes.get(index)))
        else:
            print("<<<<<<Error: No primary key!\n")

    def search(self, query):
        # filter by where
        if query.get("where") is None:
            rows_after_where = list(self.rows.values())
        else:
            #if single condition on index, use index
            condition = query.get("where")
            if condition.get("gt") is not None or condition.get("lt") is not None or condition.get("gte") is not None or condition.get("lte") is not None or condition.get("eq") is not None:

                if list(condition.values())[0][0] in self.indexes:
                    rows_after_where = self.compute_where_index(condition, list(condition.keys())[0])
                else:
                    rows_after_where = self.compute_where(query)
            else:
                rows_after_where = self.compute_where(query)

        # projection
        if "*" in query.get("select_cols"):
            if "function" in query.get("select_cols").get("*"):
                s = query.get("select_cols").get("*").get("function")
                del query.get("select_cols")["*"]
                for col in self.fields:
                    query.get("select_cols")[col] = {"index": self.fields.get(col).get("index"),"function": s}
                    break
            else:
                del query.get("select_cols")["*"]
                query.get("select_cols").update(self.fields)
            rows_with_select_cols = rows_after_where

        else:
            rows_with_select_cols = self.compute_project(rows_after_where, query)

        # group by and having
        rows_unsort = []

        if query.get("group_cols") is not None:
            rows_after_group = self.group_by(rows_with_select_cols, query.get("group_cols"), query.get("select_cols"))

            # filter by "having" conditions
            if query.get("having") is not None:
                for row in rows_after_group:
                    if self.check_conditions(row, query.get("select_cols"), query.get("having")):
                        rows_unsort.append(row)
            else:
                rows_unsort = rows_after_group
        else:
            # apply function if exists
            if self.have_function(query.get("select_cols")):
                rows_unsort = self.apply_function(rows_with_select_cols, query.get("select_cols"))
            else:
                rows_unsort = rows_with_select_cols

        # order by

        if query.get("orderby") is not None:
            rows_sort = self.order_by(rows_unsort, query.get("orderby"), query.get("select_cols"))
        else:
            rows_sort= rows_unsort

        result = Output(query.get("select_cols"), rows_sort)

        result.printOutput()
        print()

        return result

    def compute_where(self, query):
        rows_after_where = []
        for key in self.rows:
            row = self.rows.get(key)
            if self.check_conditions(row, self.fields, query.get("where")):
                rows_after_where.append(row)
        return rows_after_where

    def compute_where_index(self, condition, operator):
        rows_after_where = []
        pointers = []
        operand1 = condition.get(operator)[0]
        operand2 = condition.get(operator)[1]
        if type(operand2) is dict:
            operand2 = operand2.get("literal")
        if operator == "eq":
            # values at keys = operand2
            pointers = list(self.indexes.get(operand1).values(min = operand2, max = operand2))
        elif operator == "gt":
            # values at keys >= operand2
            pointers = list(self.indexes.get(operand1).values(min = operand2, excludemin=True))
        elif operator == "lt":
            # values at keys < operand2
            pointers = list(self.indexes.get(operand1).values(max = operand2, excludemax=True))
        elif operator == "gte":
            # values at keys >= operand2
            pointers = list(self.indexes.get(operand1).values(min=operand2))
        elif operator == "lte":
            # values at keys <= operand2
            pointers = list(self.indexes.get(operand1).values(max=operand2))
        for p in pointers:
            rows_after_where.append(self.rows.get(p))
        return rows_after_where

    def check_conditions(self, row, fields, conditions):
        if conditions is None:
            return True

        if conditions.get("and") is not None:
            for con in sorted(conditions.get("and"), key=lambda d: list(d.keys())):
                operator = list(con.keys())[0]
                operand1 = self.get_operand1(con, operator)
                operand2 = self.get_operand2(con, operator)

                if operator == "eq":
                    if row[fields.get(operand1).get("index")] != operand2:
                        return False
                elif operator == "gt":
                    if row[fields.get(operand1).get("index")] <= operand2:
                        return False
                elif operator == "lt":
                    if row[fields.get(operand1).get("index")] >= operand2:
                        return False
                elif operator == "gte":
                    if row[fields.get(operand1).get("index")] < operand2:
                        return False
                elif operator == "lte":
                    if row[fields.get(operand1).get("index")] > operand2:
                        return False
                elif operator == "or":
                    if not self.check_conditions(row, fields, con):
                        return False
            return True
        elif conditions.get("or") is not None:
            for con in conditions.get("or"):
                operator = list(con.keys())[0]
                operand1 = self.get_operand1(con, operator)
                operand2 = self.get_operand2(con, operator)
                if operator == "eq":
                    if row[fields.get(operand1).get("index")] == operand2:
                        return True
                elif operator == "gt":
                    if row[fields.get(operand1).get("index")] > operand2:
                        return True
                elif operator == "lt":
                    if row[fields.get(operand1).get("index")] < operand2:
                        return True
                elif operator == "and":
                    if self.check_conditions(row, fields, con):
                        return True
                elif operator == "gte":
                    if row[fields.get(operand1).get("index")] >= operand2:
                        return True
                elif operator == "lte":
                    if row[fields.get(operand1).get("index")] <= operand2:
                        return True
            return False
        else:
            operator = list(conditions.keys())[0]
            operand1 = self.get_operand1(conditions, operator)
            operand2 = self.get_operand2(conditions, operator)
            if operator == "gt":
                if row[fields.get(operand1).get("index")] > operand2:
                    return True
            elif operator == "lt":
                if row[fields.get(operand1).get("index")] < operand2:
                    return True
            elif operator == "eq":
                if row[fields.get(operand1).get("index")] == operand2:
                    return True
            elif operator == "gte":
                if row[fields.get(operand1).get("index")] >= operand2:
                    return True
            elif operator == "lte":
                if row[fields.get(operand1).get("index")] <= operand2:
                    return True
            return False

    def get_operand1(self, con, operator):
        operand1 = con.get(operator)[0]
        if operand1 is dict:
            for x in con:
                operand1 = con.get(x)
        return operand1

    def get_operand2(self, con, operator):
        operand2 = con.get(operator)[1]
        if type(operand2) is dict:
            operand2 = operand2.get("literal")
        return operand2

    def compute_project(self, rows_after_where, query):
        rows_with_select_cols = []
        for row in rows_after_where:
            row_with_select_cols = [None] * len(query.get("select_cols"))
            for cl in query.get("select_cols"):
                i = query.get("select_cols").get(cl).get("index")
                row_with_select_cols[i] = row[self.fields.get(cl).get("index")]
            rows_with_select_cols.append(row_with_select_cols)
        return rows_with_select_cols

    def group_by(self, rows, group_cols, select_cols):
        rest_cols = []
        for col in select_cols:
            if col not in group_cols:
                rest_cols.append(col)

        d = {}

        for row in rows:
            k = []
            for i in range(len(group_cols)):

                k.append(row[select_cols.get(group_cols[i]).get("index")])
            key = tuple(k)

            v = []
            for j in range(len(rest_cols)):
                v.append(row[select_cols.get(rest_cols[j]).get("index")])

            if key not in d:
                d[key] = []

            d[key].append(v)

        result = []
        for key in d:
            row = [None]*len(select_cols)

            for j in range(len(group_cols)):
                row[select_cols.get(group_cols[j]).get("index")] = key[j]

            for i in range(len(rest_cols)):
                function_type = select_cols.get(rest_cols[i]).get("function")
                if function_type == "sum":
                    row[select_cols.get(rest_cols[i]).get("index")] = sum(l[i] for l in d.get(key))
                elif function_type == "count":
                    row[select_cols.get(rest_cols[i]).get("index")] = len(d.get(key))
                elif function_type == "avg":
                    summation = sum(l[i] for l in d.get(key))
                    num = len(d.get(key))
                    row[select_cols.get(rest_cols[i]).get("index")] = summation/num
            result.append(row)
        return result

    def have_function(self, select_cols):
        for col in select_cols:
            if select_cols.get(col).get("function") is not None:
                return True
        return False

    def apply_function(self, rows_with_select_cols, select_cols):
        rows_after_function = []
        for col in select_cols:
            function_type = select_cols.get(col).get("function")
            row_after_function = [None]*len(select_cols)
            i = select_cols.get(col).get("index")
            if function_type == "sum":
                row_after_function[i] = sum(row[i] for row in rows_with_select_cols)
            elif function_type == "count":
                row_after_function[i] = len(rows_with_select_cols)
            elif function_type == "avg":
                summation = sum(row[i] for row in rows_with_select_cols)
                num = len(rows_with_select_cols)
                row_after_function[i] = summation/num
            elif function_type == "min":
                row_after_function[i] = min(x[i] for x in rows_with_select_cols)
            elif function_type == "max":
                row_after_function[i] = max(x[i] for x in rows_with_select_cols)
            rows_after_function.append(row_after_function[i])
        return rows_after_function

    def order_by(self, rows, order_cols, select_cols):
        for col in order_cols:
            if order_cols.get(col) == "asc":
                rows.sort(key = itemgetter(select_cols.get(col).get("index")))
            else:
                rows.sort(key=itemgetter(select_cols.get(col).get("index")), reverse=True)

        return rows

    def delete(self, query):
        # update table
        delete_rows = []
        for key in self.rows:
            row = self.rows.get(key)
            if self.check_conditions(row, self.fields, query.get("where")):
                delete_rows.append(key)

        for n in delete_rows:
            del self.rows[n]
            # update index
            self.update_index(n, "delete")
        print("Delete succeed.")

    def update(self, query):

        # if update_attributes is primary_key and if value is in list of prim_key value return
        for att in query.get("set"):
            if att == self.primary_key:
                att_value = query.get("set").get(att)
                if type(att_value) is dict:
                    att_value = att_value.get("literal")
                if att_value in self.rows.keys():
                    print("Error: Update failed due to duplicate primary key.")
                    return

        old_pk = []
        for key in self.rows:
            row = self.rows.get(key)
            if self.check_conditions(row, self.fields, query.get("where")):
                for att in query.get("set"):
                    col_value = query.get("set").get(att)
                    if type(col_value) is dict:
                        col_value = col_value.get("literal")

                    if att == self.primary_key:
                        old_pk=key

                    # updated row
                    row[self.fields.get(att).get("index")] = col_value
            self.rows[key] = row


        if len(old_pk)>0:
            temp = self.rows.get(old_pk[0])
            del self.rows[old_pk[0]]
            self.rows[temp[self.fields.get(self.primary_key).get("index")]] = temp
        print(self.rows)

        print("Update succeed.")

    def create_index(self, query):
        t = OOBTree()
        index_name = query.get("create index").get("name")
        col = query.get("create index").get("columns")
        for r in self.rows:
            value = self.rows.get(r)[self.fields.get(col)]
            t[value] = r
        self.indexes[index_name] = t
        print(list(self.indexes.get(index_name)))
        print("Create index succeed.")

    def drop_index(self, query):
        # remove index from table.indexes
        index_name = query.get("drop").get("index")
        del self.indexes[index_name]
        print("Drop index succeed.")

    def update_index(self, pk_value, type):
        if type == "insert":
            for field in self.indexes:
                if field == self.primary_key:
                    self.indexes.get(field)[pk_value] = pk_value
                else:
                    v = self.rows.get(pk_value)[self.fields.get(field)]
                    self.indexes.get(field)[v] = pk_value
            #print("Index is updated!")
        elif type == "delete":
            for field in self.indexes:
                if field == self.primary_key:
                    self.indexes.get(field).remove(pk_value)
                else:
                    v = self.rows.get(pk_value)[self.fields.get(field)]
                    self.indexes.get(field).remove(v)
            #print("Index is updated!")

    def get_value(self, ind, str):
        return self.rows.get(ind)[self.fields.get(str).get("index")]

    def printTable(self):
        cols = [None]*len(self.fields)
        for cl in self.fields:
            i = self.fields.get(cl).get("index")
            name = self.fields.get(cl).get("name")
            if name is not None:
                cols[i] = name
            else:
                cols[i] = cl


        df = pd.DataFrame(self.rows.values(), columns=cols)
        print(df)


