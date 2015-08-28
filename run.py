import sys
from dbms import Dbms


def init_shell(cursor):
    while 1:
        query = raw_input(">> ")
        cursor.execute(query)
    pass

if __name__ == '__main__':
    cursor = Dbms()
    if len(sys.argv) > 1:
        cursor.execute(sys.argv[1])
    else:
        init_shell(cursor)