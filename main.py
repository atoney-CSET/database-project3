from database import Database
from parser import Parser
import sqlvalidator

def main_routine(parser, db):
    print(">>> ", end="")
    command = input()
    if command == "exit":
        return "quit"

    arr = command.split()
    if arr[0] == "source":
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
        parsed_query = parser.parse_query(command)
        #print(parsed_query)
        db.handle(parsed_query)

    return "continue"


if __name__ == '__main__':

    db = Database("project3")
    p = Parser()

    cont = True

    while cont:
        try:
            if main_routine(p, db) == "quit":
                cont = False
        except Exception as e:
            print(f"Error: {e}")
            continue
        

    # Instantiate a parser
    # Define a list for parsed query
    # If single sql, call parser.parseSingle(), add result to the list
    # If multiple sql, call parser.parseMany(), add result to the list

    # For each parsed query in the list
        # Call handler.handle(query)

        # Print query time
        # Print output

