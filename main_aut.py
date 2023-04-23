from database import Database
from parser import Parser
import sqlvalidator
import argparse


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    args = parser.parse_args()

    db = Database("project3")
    p = Parser()

    while True:
        command = args.command
        print(command)
        if command == "exit":
            break

        arr = command.split()
        if arr[0] == "source":
            print(arr[1])
            file = open(arr[1], "r")
            for x in file:
                print(x)
                parsed_query = p.parse_query(x)
                db.handle(parsed_query)
            file.close()

        else:
            """
            s = sqlvalidator.parse(command)
            if not s.is_valid():
                print("wrong sql command")
            else:
                parsed_query = p.parse_query(command)
                #db.handle(parsed_query)
                #print(parsed_query)
                # print(command)
            """
            parsed_query = p.parse_query(command)
            #print(parsed_query)
            db.handle(parsed_query)








    # Instantiate a parser
    # Define a list for parsed query
    # If single sql, call parser.parseSingle(), add result to the list
    # If multiple sql, call parser.parseMany(), add result to the list

    # For each parsed query in the list
        # Call handler.handle(query)

        # Print query time
        # Print output
