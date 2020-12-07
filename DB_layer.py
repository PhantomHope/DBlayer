from sqlalchemy import create_engine, MetaData, Table
import os


class Repository:
    def __init__(self, str_connect, table_name):
        '''
        Creates and/or accesses existing table in the database'''
        self.__connection_string = str_connect
        self.__connection, self.__table = \
            self.__get_engine_connection(self.__connection_string, table_name)
        self.__columns = [i for i in self.__table.columns]

    @staticmethod
    def __get_engine_connection(str_connect, table_name):
        '''
        Function for getting connection to database and table

        Examples:
            connection string:
                local: sqlite:///E:\\path\\to\\database.db
                sever: sqlite:///<username>:<password>@host/dbname
        '''
        engine = create_engine(str_connect, convert_unicode=True)
        metadata = MetaData(bind=engine)

        table = Table(table_name, metadata, autoload=True)
        con = engine.connect()

        return con, table

    @staticmethod
    def __validate_columns_requests(table, params, key_error=False):
        '''
        Checking exist columns request
        '''
        if params == {}:
            return False
        result = all(i in table.columns.keys() for i in params.keys())

        if not result and key_error:
            return ", ".join(["\'" + i + "\'" for i in params.keys() if i not in table.columns.keys()])

        return result


    def insert(self, data=None, **params):
        '''
        Inserts one new record(s) into the table'''

        if self.__validate_columns_requests(self.__table, params):
            new_record = params
        elif self.__validate_columns_requests(self.__table, data):
            new_record = data
        else:
            return False

        return self.__connection.execute(self.__table.insert(), new_record)

    def update(self, pk, **params):
        '''
        Updates records in the table'''
        if self.__validate_columns_requests(self.__table, params):
            primary_key = self.__table.primary_key.columns.values()[0].name
            self.__connection.execute(
                self.__table.update(self.__table.c[primary_key]==pk),
                params
            )
        else:
            raise KeyError(
                "Key(s): " + self.__validate_columns_requests(self.__table, params, key_error=True) + " doesn't exist in table."
            )

    def delete(self, pk):
        '''
        Delete records from the table'''
        primary_key = self.__table.primary_key.columns.values()[0].name
        self.__connection.execute(
            self.__table.delete(self.__table.c[primary_key] == pk)
        )

    def select(self, *args, **kwargs):
        '''
        Select records from the table'''
        query = self.__table.select(*args, **kwargs).execute()
        return query

    def drop(self, *args, **kwargs):
        '''
        Drop the table'''
        print(self.__table.exists())
        if self.__table.exists():
            self.__table.drop(self.__connection.engine)


if __name__ == "__main__":

    # Example of usage:
    # 1. Initialize access to the table

    # print(os.getcwd()+"\\db\\chinook.db")
    my_table = Repository('sqlite:///' + os.getcwd()+"\\db\\chinook.db", 'customers')


    # 2. Insert new records

    # my_table.insert(data={
    #     'CustomerId': "1245",
    #     'FirstName': "Matthew",
    #     'LastName': "Jones",
    #     'Company': "Shell",
    #     'Address': None,
    #     'City': "New York",
    #     'State': "New York",
    #     'Country': "USA",
    #     'PostalCode': "152489",
    #     'Phone': "+10595844545",
    #     'Fax': None,
    #     'Email': "example@site.com",
    #     'SupportRepId': None
    # })
    # my_table.insert()


    # 3. Select some records

    records = my_table.select()
    print(records.first())


    #4. Update the record by primary key

    # my_table.update(pk=2, FirstName="Alfred")
    # print(my_table.select().fetchall()[1])


    # 5. Delete the row from the database

    #my_table.delete(pk=1245)

    # 6. Drop the table from the database

    #my_table.drop()
    #del my_table
