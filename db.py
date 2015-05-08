# -*- coding: utf-8 -*-

import MySQLdb
from functools import partial

con_params = {
    'db': 'todo',
    'host': 'localhost',
    'user': 'cbuser',
    'passwd': 'cbpass'
}


def connect():
    return MySQLdb.connect(**con_params)


def execute_sql(statement=None):
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(statement)
        conn.commit()
        return cursor
    except MySQLdb.OperationalError:
        return None
    else:
        conn.close()


class Query:

    def __init__(self, instance, klass):
        self.instance = instance
        self.klass = klass
        self._q = None
        self._conditions = {}
        self._order_by = None
        self._limit = None
        super(Query, self).__init__()

    def __call__(self):
        '''
            Return list of elements
        '''
        return list(self.__iter__())

    def __iter__(self):
        if self._q is None:
            raise StopIteration
        if self._conditions:
            self._q += ' ' + \
                self._parse_conditions_to_sql(**self._conditions)
            self._conditions = {}
        if self._order_by:
            self._q += ' ' + self._order_by
            self._order_by = None
        if self._limit:
            self._q += ' ' + self._limit
            self._limit = None

        for row in execute_sql(self._q):
            (*value, ) = row
            value = self.klass._value_parse_to_dict(*value)
            instance = self.klass(**value)
            instance.id = value['id']
            yield instance

    def __len__(self):
        return len(self.__call__())

    def __repr__(self):
        return str(self.__call__())

    def __getitem__(self, value):
        if isinstance(value, int):
            self._limit = 'LIMIT %i ' % value
        elif isinstance(value, slice):
            try:
                start_stop = (int(value.start), int(value.stop))
                start_number = (start_stop[0], start_stop[1] - start_stop[0])
            except ValueError:
                pass
            else:
                self._limit = 'LIMIT %s, %s ' % start_number
        return self

    def create(self, **kwargs):
        '''
            Create, save and returned instance of object
        '''
        instance = self.klass(**kwargs)
        instance.save()
        return instance

    def delete(self, id=None):
        if id is not None or self.instance.id:
            table_name = self.klass.__name__.lower()
            sql = 'DELETE FROM %s WHERE id = %s' % (table_name,
                                                    id or self.instance.id)
            execute_sql(sql)

    def get_or_create(self, **kwargs):
        instance = None
        if kwargs.get('id', None):
            instance = self.get(id=kwargs['id'])
        # instance can be empty
        if not instance:
            instance = self.create(**kwargs)
        return instance

    def get(self, **kwargs):
        sql_query = self.klass._simple_query()
        sql_query += self._parse_conditions_to_sql(**kwargs)
        try:
            (*value, ) = execute_sql(sql_query).fetchone()
        except TypeError:
            return None
        value = self.klass._value_parse_to_dict(*value)
        instance = self.klass(**value)
        instance.id = value['id']
        return instance

    def all(self):
        self._q = self.klass._simple_query()
        return self

    def filter(self, **kwargs):
        '''
        Build dict of conditions
        '''
        self._q = self.klass._simple_query()
        self._conditions.update(kwargs)
        return self

    def order_by(self, *args):
        sql_query = 'ORDER BY '
        for key in args:
            if sql_query != 'ORDER BY ':
                sql_query += ', '
            if key.startswith('-'):
                sql_query += key[1:] + ' DESC'
            else:
                sql_query += key + ' ASC'
        self._order_by = sql_query
        return self

    def count(self):
        table_name = self.klass.__name__.lower()
        sql_query = 'SELECT COUNT(*) FROM %s' % table_name
        (number, ) = execute_sql(sql_query).fetchone()
        return number

    def execute_query(self, query):
        self._q = query
        return list(self)

    @classmethod
    def _parse_conditions_to_sql(cls, **kwargs):
        sql_query = ' WHERE '
        for key, value in kwargs.items():
            if not sql_query.endswith('WHERE '):
                sql_query += ' AND '
            sql_query += '%s = \'%s\'' % (key, value)
        return sql_query


class BasicModel(type):

    def __new__(meta, classname, supers, classdict):
        classdict = meta.parse_fields(classdict)
        return type.__new__(meta, classname, supers, classdict)

    @staticmethod
    def parse_fields(classdict):
        '''
            Check if class doesn't have attribute of Fields then add it with id field
        '''
        if 'Fields' not in classdict:
            classdict['Fields'] = ('id', )
        else:
            for key in classdict:
                if key == 'Fields':
                    classdict[key] = BasicModel.check_id(classdict[key])
        return classdict

    @staticmethod
    def check_id(fields):
        if 'id' not in fields:
            return ('id', ) + fields
        return fields


class Model(metaclass=BasicModel):

    def __init__(self, *args, **kwargs):
        '''
            Create object attribute from class Fields
        '''
        self.id = None
        for field in self.__class__.Fields:
            if field != 'id':
                try:
                    self.__dict__[field] = kwargs[field]
                except KeyError:
                    self.__dict__[field] = None

    def save(self):
        if self.id is None:
            table_name = self.__class__.__name__.lower()
            sql_query = '''INSERT INTO %s (%s) values%s ''' % (
                table_name, self._parse_fields(), self._fields_values_to_str())
            cursor = execute_sql(sql_query)
            if cursor is not None:
                self.id = cursor.lastrowid
        return self

    def update(self):
        if self.id:
            execute_sql(self._create_update_sql())

    def delete(self):
        self.objects.delete()

    class _fields_values_to_str:

        '''
            Prepared tuple in string of object fields values or kwargs values
            in the order of fields
            Fields = ('id', 'list_id', 'name')
            {'name': 'Something', 'list_id': 5}
            > (5, 'Something)
        '''

        def __get__(self, instance, cls):
            def fields_values_to_str(instance=None, cls=None, **kwargs):
                if len(cls.Fields) == 1:
                    return "(NULL)"
                value = []
                if instance:
                    value_of_dict = instance.__dict__
                else:
                    value_of_dict = kwargs
                    value_of_dict['id'] = None
                for i in cls.Fields:
                    value.append(value_of_dict[i])
                return (str(tuple(value))).replace('None', 'NULL')
            return partial(fields_values_to_str, instance, cls)

    _fields_values_to_str = _fields_values_to_str()

    def _create_update_sql(self):
        table_name = self.__class__.__name__.lower()
        sql_query = 'UPDATE %s SET ' % table_name
        for i in self.__class__.Fields:
            if not sql_query.endswith('SET '):
                sql_query += ', '
            sql_query += '%s = \'%s\'' % (i, self.__dict__[i])
        sql_query += ' WHERE id = %i' % self.id
        return sql_query

    @classmethod
    def _value_parse_to_dict(cls, *value):
        '''
            Combines correct of value with fields
        '''
        dict_values = {}
        for field, value in zip(cls.Fields, value):
            dict_values[field] = value
        return dict_values

    @classmethod
    def _simple_query(cls):
        return 'SELECT %s FROM %s' % (cls._parse_fields(), cls.__name__.lower())

    @classmethod
    def _parse_fields(cls):
        '''
        > Fields = ('id', 'list_id', 'name')
        > tuple_of_fields = 'id, list_id, name'
        '''
        tuple_of_fields = ''
        for key in cls.Fields:
            if tuple_of_fields != '':
                tuple_of_fields += ', '
            tuple_of_fields += key
        return tuple_of_fields

    class objects:

        def __get__(self, instance, cls):
            return Query(instance, cls)

    objects = objects()
