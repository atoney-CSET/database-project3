from mo_sql_parsing import parse
import insert_parser


class Parser:

    def __init__(self):
        self._insert_parser = insert_parser.InsertParser()

    def should_use_custom_insert_parser(self, query):
        is_insert_statement = query[:6] == "INSERT"

        if not is_insert_statement:
            return False

        is_compound_insert = query.split("VALUES")[1].find("),") != -1

        return is_insert_statement and is_compound_insert

    def parse_query(self, query):
        # generate parsed_query using mo_sql_parsing.parse() with extra two key-value pairs - {"type": "select"} and {"inner join": true}
        if self.should_use_custom_insert_parser(query):
            q = self._insert_parser.parse(query)
        else:
            q = parse(query)

        return self.format(q)

    def format(self, query):
        # generate parsed_query using mo_sql_parsing.parse() with extra two key-value pairs - {"type": "select"} and {"inner join": true}
        types = ["select", "insert", "update", "delete", "create table", "drop", "create index"]
        for keyword in types:
            if keyword in query.keys():
                if keyword == "insert":
                    return self.format_insert(query)
                elif keyword == "select":
                    return self.format_select(query)
                elif keyword == "delete":
                    return self.format_delete(query)
                elif keyword == "update":
                    return self.format_update(query)
                elif keyword == "create table":
                    return self.format_create(query)
                elif keyword == "drop":
                    return self.format_drop(query)
                elif keyword == "create index":
                    return self.format_create_index(query)

        print("<<<<<<< Error: Command type is not supported.")
        return

    def format_create(self,query):
        parsed_query = query
        parsed_query["type"] = "create table"
        return parsed_query

    def format_drop(self, query):
        parsed_query = query
        parsed_query["type"] = "drop"
        return parsed_query
    def format_insert(self, query):
        parsed_query = {"type": "insert", "table": query.get("insert")}
        rows = []
        if query.get("query") is not None: # single row insert
            row = {}
            for col in query.get("columns"):
                temp = query.get("query").get("select")[query.get("columns").index(col)]
                if type(temp.get("value")) is dict:
                    row[col] = temp.get("value").get("literal")
                else:
                    row[col] = temp.get("value")
            rows.append(row)
        else:
            rows = query.get("values")
        parsed_query["values"] = rows
        return parsed_query

    def format_delete(self, query):
        parsed_query = {"type": "delete", "table": query.get("delete")}
        if query.get("where") is not None:
            parsed_query["where"] = query.get("where")
        return parsed_query

    def format_update(self, query):
        parsed_query = {"type": "update", "table": query.get("update")}
        if query.get("where") is not None:
            parsed_query["where"] = query.get("where")
        parsed_query["set"] = query.get("set")
        return parsed_query

    def format_select(self, query):
        parsed_query = {"type": "select"}

        if query.get("limit") is not None:
            parsed_query["limit"] = query.get("limit")

        # select_cols {"CustomerID": {"function": "count", "index": 0, "name": "CID"}, "OrderId": ...}
        select_cols = {}

        if query.get("select") == "*":
            select_cols["*"] = {"index": 0}

            parsed_query["select_col"] = select_cols
        else:
            cols = []
            if type(query.get("select")) is dict:
                cols.append(query.get("select"))
            else:
                cols = query.get("select")

            for i in range(len(cols)):
                cl = {"index": i}
                d = cols[i]
                if d.get("name") is not None:
                    cl["name"] = d.get("name")

                if type(d.get("value")) is not dict:
                    select_cols[d.get("value")] = cl
                else:
                    if d.get("value").get("count") is not None:
                        cl["function"] = "count"
                        select_cols[d.get("value").get("count")] = cl
                    elif d.get("value").get("sum") is not None:
                        cl["function"] = "sum"
                        select_cols[d.get("value").get("sum")] = cl
                    elif d.get("value").get("avg") is not None:
                        cl["function"] = "avg"
                        select_cols[d.get("value").get("avg")] = cl
                    elif d.get("value").get("min") is not None:
                        cl["function"] = "min"
                        select_cols[d.get("value").get("min")] = cl
                    elif d.get("value").get("max") is not None:
                        cl["function"] = "max"
                        select_cols[d.get("value").get("max")] = cl
        parsed_query["select_cols"] = select_cols

        if type(query.get("from")) is not list:
            parsed_query["table"] = query.get("from")
        else:
            tables = {"table1": query.get("from")[0]}

            if "left join" in query.get("from")[1]:
                tables["table2"] = query.get("from")[1].get("left join")
                tables["join type"] = "left join"
            elif "right join" in query.get("from")[1]:
                tables["table2"] = query.get("from")[1].get("right join")
                tables["join type"] = "right join"
            elif "inner join" in query.get("from")[1]:
                tables["table2"] = query.get("from")[1].get("inner join")
                tables["join type"] = "inner join"
            elif "full join" in query.get("from")[1]:
                tables["table2"] = query.get("from")[1].get("full join")
                tables["join type"] = "full join"
            else:
                print("<<<<<<< Error: join type does not supported.")
                return
            join_keys = list(query.get("from")[1].get("on").values())[0]
            for i in range(len(join_keys)):

                parts = join_keys[i].split(".")
                if parts[0] == tables["table1"]:
                    tables["jk1"] = parts[1]
                else:
                    tables["jk2"] = parts[1]
            parsed_query["table"] = tables

        # {"and":{"gt":{"ID", 3}, "or": {...}, ...}}
        if query.get("where") is not None:
            parsed_query["where"] = query.get("where")

        if query.get("groupby") is not None:
            group_cols = []  # [col_name,....]
            if type(query.get("groupby")) != dict:
                for g in query.get("groupby"):
                    group_cols.append(g.get("value"))
            else:
                group_cols.append(query.get("groupby").get("value"))
            parsed_query["group_cols"] = group_cols

        if query.get("having") is not None:
            parsed_query["having"] = query.get("having")

        # order_by {"col": "desc", ...}
        if query.get("orderby") is not None:
            order_cols = {}
            if type(query.get("orderby")) is not dict:
                for item in query.get("orderby"):
                    col = ""
                    if type(item.get("value")) is dict:
                        for n in item.get("value"):
                            col = item.get("value").get(n)
                    else:
                        col = item.get("value")

                    if item.get("sort") is not None:
                        order_cols[col] = item.get("sort")
                    else:

                        order_cols[col] = "asc"
            else:
                col = ""
                if type(query.get("orderby").get("value")) is dict:
                    for n in query.get("orderby").get("value"):
                        col = query.get("orderby").get("value").get(n)
                else:
                    col = query.get("orderby").get("value")

                if query.get("orderby").get("sort") is not None:
                    order_cols[col] = query.get("orderby").get("sort")
                else:

                    if type(col) is dict:
                        for n in col:
                            order_cols[col.get(n)] = "asc"
                    else:
                        order_cols[col] = "asc"
            parsed_query["orderby"] = order_cols

        return parsed_query

    def format_create_index(self,query):
        parsed_query = query
        parsed_query["type"] = "create index"
        parsed_query["table"] = query.get("create index").get("table")
        parsed_query["name"] = query.get("create index").get("name")
        parsed_query["columns"] = query.get("create index").get("columns")
        return parsed_query