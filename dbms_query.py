import pdb
import sqlparse
import csv
import numpy
from prettytable import PrettyTable
class TableReader:
    def __init__(self):
        self.__metadata__ = {}
        self.__load_metadata__()
        self.__current_data = {}
        pass

    def __load_metadata__(self):
        with open('metadata.txt') as f:
            content = [x.rstrip() for x in f.readlines()]
        self.__metadata__["sequence"] = {}
        new_table = 1
        current_table = ''
        for line in content:
            if line == '<end_table>':
                new_table = 1
                continue
            if line == '<begin_table>':
                continue
            if new_table == 1:
                current_table = line
                self.__metadata__[current_table] = {}
                self.__metadata__["sequence"][current_table] = []
                new_table = 0
                continue
            self.__metadata__[current_table][line] = []
            self.__metadata__["sequence"][current_table].append(line)

    def load_multi(self, tables):
        header = []
        for table in tables:
            self.load(str(table))
            header += [str(table)+'.'+x for x in self.__metadata__["sequence"][str(table)]]
        return 0, self.__current_data, header

    def clean(self):
        self.__current_data = []

    def load(self, table):
        # Loads table and return as object
        # 0 -> success
        # 1 -> table not found
        # 2 -> data not found
        error = 0
        try:
            self.__metadata__[table]
        except:
            return (1, [])
        try:
            ifile = open(table + '.csv', "rb")
        except:
            return (2,[])
        reader = csv.reader(ifile)
        i = 1
        data = []
        for row in reader:
            # row = [int(x) for x in row] + [i]
            count = 0
            for x in row:
                x = int(x)
                col_name = self.__metadata__["sequence"][table][count]
                self.__metadata__[table][col_name].append(x)
                count += 1
            i += 1
            data.append(row)
        if(len(self.__current_data)) > 0:
            new_data = []
            for i in range(len(data)):
                for j in range(len(self.__current_data)):
                    new_data.append(self.__current_data[j] + data[i])
            self.__current_data = new_data
        else:
            self.__current_data = data
        return error, data


class Select:

    def __init__(self):
        self.__query__ = ''
        self.__parsed_query__ = None
        self.__reader__ = TableReader()
        pass

    def __error_message__(self, code):
        error_msg_dict = {
            1146: "ERROR 1146 (42S02): Table doesn't exist",
            1: "ERROR 404 (1234): Data for table doesn't exist",
        }
        print error_msg_dict[code]
        return

    def __execute__(self, data):
        table_data = {}
        self.__reader__.clean()
        (error, table_data, table_header) = self.__reader__.load_multi(data[1])
        if error == 1:
            return
        self.print_nice(table_header, table_data)
        pass

    def print_nice(self, header, data):
        x = PrettyTable(header)
        for i in range(len(data)):
            x.add_row(data[i])
        print x

    def execute(self, query, parsed_query):
        self.__query__ = query
        self.__parsed_query__ = parsed_query
        # Ok ! I already know you are 'SELECT', no need to hide. Go away.
        tokens = parsed_query.tokens.pop(0)
        self.__select_parse__(parsed_query)
        pass

    def __select_parse__(self, parsed_query):
        fields = 0
        tables = 1
        where = 2
        data = [[], [], []]
        count = 0
        for i in parsed_query.tokens:
            if count == 0 and str(i).upper() == 'FROM':
                count += 1
                continue
            if str(i) == ';':
                continue
            if not i.is_whitespace():
                if isinstance(i, sqlparse.sql.IdentifierList):
                    for x in i.get_identifiers():
                        data[count].append(x)
                elif isinstance(i, sqlparse.sql.Where):
                    count += 1
                    where_data = i.flatten()
                    temp = 0
                    temp_data = [[],[],[]]
                    for x in where_data:
                        if not x.is_whitespace():
                            if isinstance(x, sqlparse.sql.Token):
                                if not str(x).upper() == 'WHERE':
                                    if str(x).upper() == 'AND' or str(x).upper() == 'OR':
                                        data[count].append(temp_data)
                                        temp_data = [[],[],[]]
                                        temp = 0
                                        data[count].append(x)
                                        continue
                                    if str(x) == '.':
                                        temp_data[temp-1].append(x)
                                        temp -= 1
                                    else:
                                        if temp == 3:
                                            continue
                                        temp_data[temp].append(x)
                                        temp += 1
                            # else:
                            #     data[count].append(i)
                            # if isinstance(x, sqlparse.sql.Token):
                            #     data[count].append(i)
                    data[count].append(temp_data)
        self.__execute__(data)
        # pdb.set_trace()
        # for words in tokens:
        #     print words
        pass