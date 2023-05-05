from table import Table
from BTrees.OOBTree import OOBTree
from operator import itemgetter
import pandas as pd
from datetime import datetime


class Database:

    def __init__(self, name):
        self.tables = {}
        self.name = name

    def showTables(self):
        col = ['Tables']
        rows = list(self.tables.keys())

        df = pd.DataFrame(rows, columns=col)
        print(df)

    def add(self, table):
        self.tables[table.name] = table
        print("<<<<<<< Table is created.")
        self.showTables()

    def drop(self, table_name):
        if self.tables.get(table_name) is not None:
            del self.tables[table_name]
            print("<<<<<<< Table is dropped.\n")
            self.showTables()
            print()
        else:
            print("<<<<<<< Error: Table does not exist.")

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
                print("<<<<<<< Error: Table exists.")
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

            print("<<<<<<< Table is created.\n")
            self.showTables()
            print()

        elif sql_type == "drop":
            if "table" in query.get("drop").keys():
                table_name = query.get("drop").get("table")
                self.drop(table_name)
                # del self.tables[table_name]
                #print("<<<<<<Table is dropped.\n")
                #self.showTables()

            elif "index" in query.get("drop").keys():
                index_name = query.get("drop").get("index")
                for t in self.tables:
                    if index_name in self.tables.get(t).indexes:
                        del self.tables.get(t).indexes[index_name]
                        self.tables.get(t).showIndexes()
                        print("<<<<<<< Index is dropped.\n")

        elif sql_type == "insert":
            """
            table_name = query.get("table")
            self.tables.get(table_name).insert(query)
            """
            table = self.tables.get(query.get("table"))
            rows = query.get("values")

            if table.primary_key is not None:
                for row in rows:
                    pk_value = row.get(table.primary_key)
                    if table.rows.get(pk_value) is not None:
                        print("<<<<<<< Error: Duplicate record. Primary key exists.")

                    else:
                        if table.foreign_key is not None and table.reference is not None:
                            ref_table = self.tables.get(table.reference)
                            ind = ref_table.fields.get(table.foreign_key).get("index")
                            lst = list(ref_table.rows.values())
                            values = [item[ind] for item in lst]
                            if row.get(table.foreign_key) in values:
                                table.insert(row, pk_value)
                            else:
                                print("<<<<<< Error: value does not exist in the foreign column it references.")
                        else:
                            table.insert(row, pk_value)

            else:
                print("<<<<<<< Error: No primary key.")

        elif sql_type == "delete":
            table_name = query.get("table")
            self.tables.get(table_name).delete(query)

        elif sql_type == "update":
            table_name = query.get("table")
            self.tables.get(table_name).update(query)

        elif sql_type == "create index":
            table_name = query.get("table")
            self.tables.get(table_name).create_index(query)

        elif sql_type == "select":
            if type(query.get("table")) is dict:
                table1 = self.tables.get(query.get("table").get("table1"))
                table2 = self.tables.get(query.get("table").get("table2"))
                #t1 = list(self.tables.get(query.get("table").get("table1")).rows.values())
                #t2 = list(self.tables.get(query.get("table").get("table2")).rows.values())
                jk1 = query.get("table").get("jk1")
                jk2 = query.get("table").get("jk2")
                #f1 = self.tables.get(query.get("table").get("table1")).fields
                #f2 = self.tables.get(query.get("table").get("table2")).fields
                join_type = query.get("table").get("join type")
                if join_type == "inner join":
                    start_time = datetime.now()
                    t3_nest = self.inner_join_nest(table1, table2, jk1, jk2)
                    t3_nest.search(query)
                    end_time = datetime.now()
                    print('(Nest inner join execution time: {} s)'.format(end_time - start_time))
                    print()

                    start_time = datetime.now()
                    t3_sort = self.inner_join_sort(table1, table2, jk1, jk2)
                    t3_sort.search(query)
                    end_time = datetime.now()
                    print('(Sort inner join execution time: {} s)'.format(end_time - start_time))
                    print()

                elif join_type == "left join":
                    start_time = datetime.now()
                    t3_nest = self.left_join_nest(table1, table2, jk1, jk2)
                    t3_nest.search(query)
                    end_time = datetime.now()
                    print('(Nest left join execution time: {} s)'.format(end_time - start_time))
                    print()

                    start_time = datetime.now()
                    t3_sort = self.left_join_sort(table1, table2, jk1, jk2)
                    t3_sort.search(query)
                    end_time = datetime.now()
                    print('(Sort left join execution time: {} s)'.format(end_time - start_time))
                    print()

                elif join_type == "right join":
                    start_time = datetime.now()
                    t3_nest = self.right_join_nest(table1, table2, jk1, jk2)
                    t3_nest.search(query)
                    end_time = datetime.now()
                    print('(Nest right join execution time: {} s)'.format(end_time - start_time))
                    print()

                    start_time = datetime.now()
                    t3_sort = self.right_join_sort(table1, table2, jk1, jk2)
                    t3_sort.search(query)
                    end_time = datetime.now()
                    print('(Sort right join execution time: {} s)'.format(end_time - start_time))
                    print()

                elif join_type == "full join":
                    start_time = datetime.now()
                    t3_nest = self.full_join_nest(table1, table2, jk1, jk2)
                    t3_nest.search(query)
                    end_time = datetime.now()
                    print('(Nest full join execution time: {} s)'.format(end_time - start_time))
                    print()

                    start_time = datetime.now()
                    t3_sort = self.full_join_sort(table1, table2, jk1, jk2)
                    t3_sort.search(query)
                    end_time = datetime.now()
                    print('(Sort full join execution time: {} s)'.format(end_time - start_time))
                    print()
                else:
                    print("<<<<<<< Error: Join type does not support!")

            else:
                self.tables.get(query.get("table")).search(query)

        else:
            print("<<<<<<< Error: SQL type is not supported.")
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

        counter = 0
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

                    new_rows[counter] = row
                    counter = counter+1

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
        counter = 0
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                t = j
                while j < len(t2) and t1[i][jk1_index] == t2[j][jk2_index]:
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter = counter+1
                    j = j + 1
                j = t
                i = i + 1
                #j = j + 1
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
        counter = 0
        matchedID_table1 = []

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    matchedID_table1.append(i)
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter = counter+1

        for i in range(len(t1)):
            if i not in matchedID_table1:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter+1

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
        counter = 0
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                t = j
                while j < len(t2) and t1[i][jk1_index] == t2[j][jk2_index]:
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter = counter + 1
                    j = j + 1
                j = t
                i = i + 1
                # j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                j = j + 1
            else:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter + 1
                i = i + 1

        while i < len(t1):
            row = [None] * len(fields)
            for f in fields:
                if f in table1.fields:
                    row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
            new_rows[counter] = row
            counter = counter + 1
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

        matchID_t2 = []
        counter=0
        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    # add row
                    matchID_t2.append(j)
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter=counter+1

        for j in range(len(t2)):
            if j not in matchID_t2:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter + 1

        table3.rows = new_rows

        return table3
    def right_join_sort(self, table1, table2, jk1, jk2):
        return self.left_join_sort(table2, table1, jk2, jk1)


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

        counter=0
        matchID_t1=[]
        matchID_t2=[]

        for i in range(len(t1)):
            for j in range(len(t2)):
                if t1[i][jk1_index] == t2[j][jk2_index]:
                    matchID_t1.append(i)
                    matchID_t2.append(j)
                    # add row
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter=counter+1

        for i in range(len(t1)):
            if i not in matchID_t1:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter+1

        for j in range(len(t2)):
            if j not in matchID_t2:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter + 1

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
        matchID_t1 = []
        matchID_t2 = []
        counter = 0
        i = 0
        j = 0
        while i < len(t1) and j < len(t2):
            if t1[i][jk1_index] == t2[j][jk2_index]:
                # add row
                t = j
                while j < len(t2) and t1[i][jk1_index] == t2[j][jk2_index]:
                    if i not in matchID_t1:
                        matchID_t1.append(i)
                    if j not in matchID_t2:
                        matchID_t2.append(j)
                    row = [None] * len(fields)
                    for f in fields:
                        if f in table1.fields:
                            row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]

                        if f in table2.fields:
                            row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]

                    new_rows[counter] = row
                    counter = counter + 1
                    j = j + 1
                j = t
                i = i + 1
                # j = j + 1
            elif t1[i][jk1_index] > t2[j][jk2_index]:
                j = j + 1
            else:
                i = i + 1

        for i in range(len(t1)):
            if i not in matchID_t1:
                row = [None] * len(fields)
                for f in fields:
                    if f in table1.fields:
                        row[fields.get(f).get("index")] = t1[i][table1.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter+1

        for j in range(len(t2)):
            if j not in matchID_t2:
                row = [None] * len(fields)
                for f in fields:
                    if f in table2.fields:
                        row[fields.get(f).get("index")] = t2[j][table2.fields.get(f).get("index")]
                new_rows[counter] = row
                counter = counter + 1

        table3.rows = new_rows

        return table3

        """
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
        """

