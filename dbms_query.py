import pdb
import sqlparse
import csv
import copy

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
        error = 0
        for table in tables:
            try:
                self.load(str(table))
                header += [str(table)+'.'+x for x in self.__metadata__["sequence"][str(table)]]
            except:
                error = 1
                print 'Table couldn\'t be loaded'
        return error, self.__current_data, header

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
        self.__dont_print = []
        self.__error__ = ''
        self.__multi = True
        self.__ignore = []
        self.__parsed_query__ = None
        self.__dont_print = []
        self.__function_on = []
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
        (error, self.__table_data, self.__table_header) = self.__reader__.load_multi(data[1])
        if error == 1:
            return
        # try:
        if len(data[2]) > 0:
            self.__parse_wheres__(data[2])
        # except:
        #     # where not present
        #     pass
        if len(self.__error__)>1:
            print self.__error__

        self.print_logic(data)


    def __parse_wheres__(self, conditions):
        method = []
        temp_data = []
        count = 0
        for i in range(len(conditions)):
            if count % 2 == 0:
                temp_data.append(self.__run_where__(conditions[i]))
            else:
                method.append(str(conditions[i]).upper())
            count += 1
        if len(self.__error__) > 1:
            return -1
        for i in range(len(method)):
            if method[i] == 'AND':
                temp_data[0] = self.__concat_table__(temp_data[0], temp_data[i+1])
            elif method[i] == 'OR':
                temp_data[0] = self.__intersect_table__(temp_data[0], temp_data[i+1])

        self.__table_data = temp_data[0]

    def __run_where__(self, condition):
        temp = copy.deepcopy(self.__table_data)
        a,b,c, digit = '','',0, False

        for i in range(len(condition[0])):
            a += str(condition[0][i])

        for i in range(len(condition[2])):
            b += str(condition[2][i])

        a = self.__find_row__(a)
        d = str(condition[1][0])
        if b.isdigit() or (b.startswith('-') and b[1:].isdigit()):
            c = b
            digit = True
        else:
            b = self.__find_row__(b)
            if d == '=':
                self.__dont_print.append(b)


        if len(self.__error__) > 1:
            return -1

        for i in list(temp):
            if digit and self.compare(i[a], c, d):
                pass
            elif (not digit) and self.compare(i[a], i[b], d):
                pass
            else:
                temp.remove(i)

        return temp

    def compare(self, a, b, c):
        a,b = int(a), int(b)
        if c == '>':
            return a>b
        if c == '<':
            return a<b
        if c == '=':
            return a==b
        if c == '<>':
            return a!=b
        if c == '>=':
            return a>=b
        if c == '<=':
            return a<=b
        if c == 'NOT':
            return a!=b
        return False

    def __find_row__(self, name):
        try:
            return self.__table_header.index(name)
        except:
            index, found = -1, 0
            for i in range(len(self.__table_header)):
                if ('.'+name) in (self.__table_header[i]):
                    index, found = i, found+1
            if found == 1:
                return index
            else:
                self.__error__ = 'Ambiguous coloum name in where statement.'
                return -1

    def __concat_table__(self, table1, table2):
        table1 = copy.deepcopy(table1)
        table2 = copy.deepcopy(table2)
        for i in list(table1):
            try:
                table2.index(i)
            except:
                table1.pop(table1.index(i))
        return table1

    def __intersect_table__(self, table1, table2):
        table1 = copy.deepcopy(table1)
        table2 = copy.deepcopy(table2)
        for i in list(table2):
            try:
                table1.index(i)
            except:
                table1.append(i)
        return table1

    def print_logic(self, data):
        coloums = data[0]
        self.__only_print = []
        function = False
        for i in coloums:
            if isinstance(i, sqlparse.sql.Function):
                function = True
                self.__function = i
            elif isinstance(i, sqlparse.sql.Identifier):
                x = self.__find_row__(str(i))
                if len(self.__error__):
                    print 'Ambiguos '+str(i)+' in SELECT...'
                    return
                else:
                    self.__only_print.append(x)
        if function == True:
            func_name = str(self.__function.tokens[0]).upper()
            col = str(self.__function.tokens[1]).upper()
            col = col.lstrip('(')
            col = col.rstrip(')')
            self.tweek_data(func_name, col)
        self.print_nice(self.__table_header, self.__table_data)

    def tweek_data(self, func, col):
        data = self.__table_data
        col = self.__find_row__(col)
        if col == -1:
            self.__error__ = 'Please provide coloum name to function in select'
        value = 0
        if func == 'MIN':
            value = 10000000000
        if func == 'MAX':
            value = -10000000000
        count = 0
        for i in range(len(data)):
            if func != 'DISTINCT':
                value = self.functions(float(data[i][col]), float(value), func)
            count +=1
        if func == 'AVG':
            value = value/count
        if func == 'DISTINCT':
            self.__multi = True
            done = []
            self.__ignore = []
            for i in range(len(data)):
                if data[i][col] in done:
                    self.__ignore.append(i)
                    pass
                else:
                    done.append(data[i][col])
        else:
            self.__multi = False
            for i in range(len(data)):
                data[i][col] = value

        self.__function_on.append(col)

    def functions(self, new, old, func):
        if func == 'MAX':
            if new>old:
                return new
            else:
                return old
        if func == 'MIN':
            if new<old:
                return new
            else:
                return old
        if func == 'SUM':
            return new + old
        if func == 'AVG':
            return new + old

    def print_nice(self, header, data):
        if len(self.__error__)>0:
            print self.__error__
            return
        count = 0
        for j in range(len(header)):
            if (j in self.__dont_print) or ((len(self.__only_print) > 0) and (j not in self.__only_print)) or ((len(self.__function_on)>0) and (j not in self.__function_on)):
                try:
                    header.pop(j-count)
                    count+=1
                except:
                    pass

        x = PrettyTable(header)
        for i in range(len(data)):
            count = 0
            if i in self.__ignore:
                continue
            for j in range(len(data[i])):
                if (j in self.__dont_print) or (len(self.__only_print) > 0 and (j not in self.__only_print)) or ((len(self.__function_on)>0) and (j not in self.__function_on)):
                    try:
                        data[i].pop(j-count)
                        count+=1
                    except:
                        pass
            x.add_row(data[i])
            if not self.__multi:
                break
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
                elif isinstance(i, sqlparse.sql.Identifier) or isinstance(i, sqlparse.sql.Function):
                    data[count].append(i)
        self.__execute__(data)
        # pdb.set_trace()
        # for words in tokens:
        #     print words
        pass