import readline
from database import Database
from parser import Parser
import timeit
import traceback

readline.parse_and_bind('tab: complete')

def main_routine(parser, db):

    command = input("sql> ")
    if command.strip() == "exit" or command.strip() == "quit":
        return "quit"

    commands = []

    arr = command.split()
    if arr[0] == "source":
        file = open(arr[1], "r")
        for x in file:
            commands.append(x)
        file.close()
    else:
        commands.append(command)

    for command in commands:
        start = timeit.default_timer()
        parsed_query = parser.parse_query(command)
        if len(command) < 60:
            print(command)
        else:
            print(command[:60] + " ... [truncated]")
        db.handle(parsed_query)
        stop = timeit.default_timer()
        print(f'(Execution Time: {stop - start:0.3f} s)')
        print("----------------------------------------------------------")
        print()

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
            print(traceback.format_exc())
            continue

