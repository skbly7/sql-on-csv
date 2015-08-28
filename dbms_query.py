
class Select:

    def __init__(self):
        self.__query__ = ''
        self.__parsed_query__ = None
        pass

    def execute(self, query, parsed_query):
        self.__query__ == query
        self.__parsed_query__ = parsed_query
        print 'Executing -> ', str(query)
        pass

