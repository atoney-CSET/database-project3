from table import Table
from BTrees.OOBTree import OOBTree
from operator import itemgetter


class Database:

    def __init__(self, name):
        self.tables = {}
        self.name = name

    def add(self, table):
        self.tables[table.name] = table
        print("Create table successfully!")

    def drop(self, table_name):
        if self.tables.get(table_name) is not None:
            del self.tables[table_name]
            print("Drop table successfully!")
        else:
            print("Error: Table " + table_name + " does not exist.")

    def handle(self, query):
        sql_type = query.get("type")

        if sql_type == "create table":

            table_name = query.get("create table").get("name")
            constraint = query.get("create table").get("constraint")
            primary_key = None
            foreign_key = None
            reference_table = None
            indexes = {}

            if self.tables.get(table_name) is not None:
                print("Error: Table " + table_name + " exists.")
                return

            if len(constraint) == 1:
                if constraint.get("primary_key") is not None:
                    primary_key = constraint.get("primary_key").get("columns")
                    index = OOBTree()
                    indexes[primary_key] = index
                if constraint.get("foreign_key") is not None:
                    foreign_key = constraint.get("foreign_key").get("columns")
                    reference_table = constraint.get("foreign_key").get("references").get("table")

            elif len(constraint) > 1:
                for con in constraint:
                    if con.get("primary_key") is not None:
                        primary_key = con.get("primary_key").get("columns")
                        index = OOBTree()
                        indexes[primary_key] = index
                    if con.get("foreign_key") is not None:
                        foreign_key = con.get("foreign_key").get("columns")
                        reference_table = con.get("foreign_key").get("references").get("table")

            fields = {}
            columns = query.get("create table").get("columns")
            for col in columns:
                field = {"index": columns.index(col)}
                fields[col.get("name")] = field

            t = Table(table_name, fields, primary_key, foreign_key, reference_table, indexes)
            self.tables[table_name] = t

            print("<<<<<Table is created.\n")
            return

        elif sql_type == "drop":
            if "table" in query.get("drop").keys():
                table_name = query.get("drop").get("table")
                self.drop(table_name)
                return
            elif "index" in query.get("drop").keys():
                print("Todo: implement drop index. mo-sql-parsing does not provide table name of the index. \n")
                # To do
                return

        elif sql_type == "select":
            if type(query.get("table")) is dict:
                table1 = self.tables.get(query.get("table").get("table1"))
                table2 = self.tables.get(query.get("table").get("table2"))
                jk1 = query.get("table").get("jk1")
                jk2 = query.get("table").get("jk2")
                joined_table1 = {}
                # joined_table2 = {}
                if query.get("table").get("join type") == "left join":
                    joined_table1 = self.left_join_sort(table1, table2, jk1, jk2)
                    # joined_table1 = self.left_join_next()
                elif query.get("table").get("join type") == "right join":
                    joined_table1 = self.right_join_sort(table1, table2, jk1, jk2)
                    # joined_table1 = self.right_join_sort(table1, table2, jk1, jk2)
                elif query.get("table").get("join type") == "inner join":
                    joined_table1 = self.inner_join_sort(table1, table2, jk1, jk2)
                    # joined_table1 = self.inner_join_sort(table1, table2, jk1, jk2)
                elif query.get("table").get("join type") == "full join":
                    joined_table1 = self.full_join_sort(table1, table2, jk1, jk2)
                    # joined_table1 = self.inner_join_sort(table1, table2, jk1, jk2)
                return joined_table1.search(query)
            else:
                return self.tables.get(query.get("table")).search(query)

        elif sql_type == "insert":
            table_name = query.get("table")
            self.tables.get(table_name).insert(query)
            return

        elif sql_type == "delete":
            table_name = query.get("table")
            self.tables.get(table_name).delete(query)
            return

        elif sql_type == "update":
            table_name = query.get("table")
            self.tables.get(table_name).update(query)
            return

        elif sql_type == "create index":
            table_name = query.get("table")
            self.tables.get(table_name).create_index(query)
            return

        else:
            print("Error: SQL type" + sql_type + "is not supported.")
            return

    #################       INNER JOIN       #################
    def inner_join_nest(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[t1[i][jk1_index]] = row

        table3.rows = new_rows

        return table3
    def inner_join_sort(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")
        t1.sort(key=itemgetter(jk1_index))
        t2.sort(key=itemgetter(jk2_index))

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                new_rows[t1[i][jk1_index]] = row
                i = i + 1
                j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                j = j + 1
            else:
                i = i + 1

        table3.rows = new_rows

        return table3

    #################       LEFT JOIN       #################
    def left_join_nest(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[t1[i][jk1_index]] = row

        for i in range(len(t1)):
            if t1[i][jk1_index] not in new_rows:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[t1[i][jk1_index]] = row

        table3.rows = new_rows

        return table3
    def left_join_sort(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")
        t1.sort(key=itemgetter(jk1_index))
        t2.sort(key=itemgetter(jk2_index))

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                new_rows[t1[i][jk1_index]] = row
                i = i + 1
                j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                j = j + 1
            else:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                    new_rows[t1[i][jk1_index]] = row
                i = i + 1

        while i < len(t1):
            row = [None] * len(fields)
            for f in fields:
                if f in table1.fields:
                    row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[t1[i][jk1_index]] = row
            i = i + 1

        table3.rows = new_rows

        return table3

    #################       RIGHT JOIN       #################
    def right_join_nest(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[t1[i][jk1_index]] = row

        for j in range(len(t2)):
            if t2[j][jk2_index] not in new_rows:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[t2[j][jk2_index]] = row

        table3.rows = new_rows

        return table3
    def right_join_sort(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")
        t1.sort(key=itemgetter(jk1_index))
        t2.sort(key=itemgetter(jk2_index))

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                new_rows[t1[i][jk1_index]] = row
                i = i + 1
                j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                    new_rows[t2[j][jk2_index]] = row
                j = j + 1
            else:
                i = i + 1

        while j < len(t2):
            row = [None] * len(fields)
            for f in fields:
                if f in table2.fields:
                    row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[t2[j][jk2_index]] = row
            j = j + 1

        table3.rows = new_rows

        return table3

    #################       FULL JOIN       #################
    def full_join_nest(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[t1[i][jk1_index]] = row


        for i in range(len(t1)):
            if t1[i][jk1_index] not in new_rows:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[t1[i][jk1_index]] = row

        for j in range(len(t2)):
            if t2[j][jk2_index] not in new_rows:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[t2[j][jk2_index]] = row

        table3.rows = new_rows

        return table3
    def full_join_sort(self, table1, table2, jk1, jk2):
        t1 = list(table1.rows.values())
        t2 = list(table2.rows.values())
        jk1_index = table1.fields.get(jk1).get("index")
        jk2_index = table2.fields.get(jk2).get("index")
        t1.sort(key=itemgetter(jk1_index))
        t2.sort(key=itemgetter(jk2_index))

        fields = dict(table1.fields)
        temp = dict(table2.fields)
        for f in temp:
            if f not in fields:
                fields[f] = {"index": len(fields)}

        table3 = Table("join_table", fields, None, None, None, None)
        new_rows = {}
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                new_rows[t1[i][jk1_index]] = row
                i = i + 1
                j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                    new_rows[t2[j][jk2_index]] = row
                j = j + 1
            else:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                    new_rows[t1[i][jk1_index]] = row
                i = i + 1

        while j < len(t2):
            row = [None] * len(fields)
            for f in fields:
                if f in table2.fields:
                    row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[t2[j][jk2_index]] = row
            j = j + 1

        while i < len(t1):
            row = [None] * len(fields)
            for f in fields:
                if f in table1.fields:
                    row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[t1[i][jk1_index]] = row
            i = i + 1

        table3.rows = new_rows

        return table3

