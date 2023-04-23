import unittest

from database import Database
from parser import Parser
from table import Table
from BTrees.OOBTree import OOBTree


class TestParser(unittest.TestCase):
    def test_parse_insert(self):
        p = Parser()
        #print(p.parse_query("INSERT INTO Clients (ClientID, ClientAge) VALUES ('1', 20);"))
        #print(p.parse_query("INSERT INTO Clients (ClientID, ClientAge) VALUES ('2', 30), ('3', 25), ('4', 50);"))

    def test_parse_delete(self):
        p = Parser()
        #print(p.parse_query("DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"))
        #print(p.parse_query("DELETE FROM Customers;"))

    def test_parse_update(self):
        p = Parser()
        #print(p.parse_query("UPDATE Clients SET ClientID = 6 WHERE ClientID = 1;"))
        #print(p.parse_query("UPDATE Customers SET ContactName = 'Alfred Schmidt', City= 'Frankfurt' WHERE CustomerID = 1;"))

    def test_parse_select(self):
        p = Parser()
        #print(p.parse_query("SELECT * FROM table1;"))
        #print(p.parse_query("SELECT ID FROM table1;"))
        #print(p.parse_query("SELECT COUNT(ID) FROM table1"))
        #print(p.parse_query("SELECT ID, COUNTRY FROM table1;"))
        #print(p.parse_query("SELECT COUNT(ID), COUNTRY FROM table1;"))

        #self.assertEqual(True, False)  # add assertion here

class TestTable(unittest.TestCase):
    def test_table_checkConditions(self):
        rows = {"1": ["1", 20, 30], "2": ["2", 20, 61], "3": ["3", 26, 30], "4": ["4", 55, 11], "5": ["5", 12, 43], "6": ["6", 1, 11]}
        fields = {"ID": {"index":0}, "Age": {"index":1}, "Salary": {"index":2}}
        bt = OOBTree()
        bt.update({"3": "3", "4": "4", "1": "1", "2": "2", "5": "5", "6": "6"})
        t = Table("table1", fields, "pk", "fk", "ref", {"ID":bt})
        t.rows = rows
        p = Parser()
        bt = OOBTree()

        #print(t.search(p.parse_query("SELECT * FROM table1;")).rows)
        #print(t.search(p.parse_query("SELECT ID FROM table1;")).rows)

        #print(t.search(p.parse_query("SELECT ID, Salary FROM table1;")).rows)

        #print(t.search(p.parse_query("SELECT ID, Age FROM table1 WHERE ID>'3';")).rows)
        #print(t.search(p.parse_query("SELECT ID, Age FROM table1 WHERE ID>'3' AND Age>20;")).rows)
        #print(t.search(p.parse_query("SELECT ID, Age, Salary FROM table1 WHERE Age>20 and Salary>20;")).rows)
        #print(t.search(p.parse_query("SELECT ID, Age, Salary FROM table1 WHERE Age >= 20 and ( Salary > 30 OR Salary < 20);")).rows)
        #print(t.search(p.parse_query("SELECT ID, Age, Salary FROM table1 WHERE Age>20 OR Age<20;")).rows)

    def test_group_by(self):
        rows = {"1": ["1", 20, 30], "2": ["2", 20, 61], "3": ["3", 26, 30], "4": ["4", 55, 11], "5": ["5", 12, 43],
                "6": ["6", 1, 11], }
        fields = {"ID": {"index":0}, "Age": {"index":1}, "Salary": {"index":2}}

        t = Table("table1", fields, "pk", "fk", "ref", None)
        t.rows = rows
        p = Parser()

        #print(t.search(p.parse_query("SELECT * FROM table1;")).rows)
        #print(t.search(p.parse_query("SELECT Age, AVG(Salary) FROM table1 WHERE Age>=20 GROUP BY Age;")).rows)
        #print(t.search(p.parse_query("SELECT COUNT(ID), Age, AVG(Salary) FROM table1 WHERE Age>=20 GROUP BY Age HAVING AVG(Salary)>20;")).rows)

    def test_order_by(self):
        rows = {"1": ["1", 20, 30], "2": ["2", 20, 61], "3": ["3", 26, 30], "4": ["4", 55, 11], "5": ["5", 12, 43],
                "6": ["6", 1, 11], }
        fields = {"ID": {"index": 0}, "Age": {"index": 1}, "Salary": {"index": 2}}

        t = Table("table1", fields, "pk", "fk", "ref", None)
        t.rows = rows
        p = Parser()

        #print(t.search(p.parse_query("SELECT * FROM table1 ORDER BY Age ASC, Salary DESC;")).rows)
        #print(t.search(p.parse_query("SELECT Age, AVG(Salary) FROM table1 WHERE Age>=20 GROUP BY Age;")).rows)
        #print(t.search(p.parse_query("SELECT Age, AVG(Salary) FROM table1 WHERE Age>=20 GROUP BY Age ORDER BY Age, AVG(Salary) DESC;")).rows)
        #print(t.search(p.parse_query("SELECT COUNT(ID), Age, AVG(Salary) FROM table1 WHERE Age>=20 GROUP BY Age HAVING AVG(Salary)>20;")).rows)

    def test_apply_function(self):
        rows = {"1": ["1", 20, 30], "2": ["2", 20, 61], "3": ["3", 26, 30], "4": ["4", 55, 11], "5": ["5", 12, 43],
                "6": ["6", 1, 11], }
        fields = {"ID": {"index": 0}, "Age": {"index": 1}, "Salary": {"index": 2}}

        t = Table("table1", fields, "pk", "fk", "ref", None)
        t.rows = rows
        p = Parser()

        #print(t.search(p.parse_query("SELECT COUNT(*) FROM table1 WHERE Age>20 AND Salary>20;")).rows)

class TestDatabase(unittest.TestCase):
    # def test_handle_create_table(self):

    # def test_handle_drop_table(self):

    def test_handle_insert_delete_update(self):
        fields = {"ClientID": {"index": 0}, "ClientAge": {"index": 1}}
        t = Table("Clients", fields, "ClientID", None, None, {})
        p = Parser()
        db = Database("test")
        db.add(t)
        sql = "INSERT INTO Clients (ClientID, ClientAge) VALUES ('1', 20);"
        sql1 = "INSERT INTO Clients (ClientID, ClientAge) VALUES ('2', 30), ('3', 25), ('4', 50);"
        sql2 = "INSERT INTO Clients (ClientID, ClientAge) VALUES ('1', 20);"
        sql3 = "DELETE FROM Clients WHERE ClientID='1';"
        #sql4 = "UPDATE Clients SET ClientAge = 60 WHERE ClientID = '4';"
        sql4 = "UPDATE Clients SET ClientID = '3' WHERE ClientID = '4';"
        parsed_sql = p.parse_query(sql)
        parsed_sql1 = p.parse_query(sql1)
        parsed_sql2 = p.parse_query(sql2)
        parsed_sql3 = p.parse_query(sql3)
        parsed_sql4 = p.parse_query(sql4)

        print(sql)
        db.handle(parsed_sql)
        t.printTable()
        #print(t.indexes.get(t.primary_key))
        print()
        print(sql1)
        db.handle(parsed_sql1)
        t.printTable()
        #print(t.indexes.get(t.primary_key))
        print()
        print(sql2)
        db.handle(parsed_sql2)
        t.printTable()
        print()
        print(sql3)
        db.handle(parsed_sql3)
        t.printTable()
        print()
        print(sql4)
        db.handle(parsed_sql4)
        t.printTable()


    #def test_handle_select(self):


    # def test_handle_join(self):

    # def test_handle_create_index(self):

    # def test_handle_drop_index(self):

    # def test_update_index(self):


    def test_join_sort(self):
        rows1 = {"1": ["1", 20], "2": ["2", 26], "3": ["3", 20], "4": ["4", 55], "5": ["5", 12],
                    "6": ["6", 18]}
        fields1 = {"ID": {"index": 0}, "Age": {"index": 1}}

        t1 = Table("table1", fields1, "pk", "fk", "ref", None)
        t1.rows = rows1

        #rows2 = {"1": ["1", 30], "2": ["2", 61], "3": ["3", 30], "4": ["4", 11], "5": ["5", 43], "6": ["6", 11]}
        rows2 = {"2": ["2", 61], "5": ["5", 43], "6": ["6", 11], "7": ["7", 11]}
        fields2 = {"ID": {"index": 0}, "Salary": {"index": 1}}

        t2 = Table("table1", fields2, "pk", "fk", "ref", None)
        t2.rows = rows2


        db = Database("test")
        #print(db.left_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.right_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.inner_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.inner_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.inner_join_nest(t1, t2, "ID", "ID").rows)

        #print(db.right_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.right_join_nest(t1, t2, "ID", "ID").rows)

        #print(db.left_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.left_join_nest(t1, t2, "ID", "ID").rows)

        #print(db.full_join_sort(t1, t2, "ID", "ID").rows)
        #print(db.full_join_nest(t1, t2, "ID", "ID").rows)


    def test_handle_join(self):
        rows1 = {"1": ["1", 20], "2": ["2", 26], "3": ["3", 20], "4": ["4", 55], "5": ["5", 12],
                 "6": ["6", 18]}
        fields1 = {"ID": {"index": 0}, "Age": {"index": 1}}

        t1 = Table("table1", fields1, "pk", "fk", "ref", None)
        t1.rows = rows1

        # rows2 = {"1": ["1", 30], "2": ["2", 61], "3": ["3", 30], "4": ["4", 11], "5": ["5", 43], "6": ["6", 11]}
        rows2 = {"2": ["2", 61], "5": ["5", 43], "6": ["6", 11], "7": ["7", 11]}
        fields2 = {"ID": {"index": 0}, "Salary": {"index": 1}}

        t2 = Table("table1", fields2, "pk", "fk", "ref", None)
        t2.rows = rows2

        db = Database("test")
        db.tables["table1"] = t1
        db.tables["table2"] = t2
        p = Parser()
        #parsed_query = p.parse_query("SELECT ID, Age, Salary FROM table1 RIGHT JOIN table2 ON table1.ID=table2.ID")
        #print(parsed_query)
        #print(db.handle(parsed_query).rows)


if __name__ == '__main__':
    unittest.main()
