# -*- coding: utf-8 -*-

import MySQLdb

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


def BasicQuery(classname, supers, classdict):
    '''
        This metafunc provides always new query instance
    '''
    aClass = type(classname, supers, classdict)

    class Factory:

        def __get__(self, instance, cls):
            return aClass(instance, cls)

    return Factory


class Query(metaclass=BasicQuery):

    def __init__(self, instance, klass):
        self.instance = instance
        self.klass = klass
        self._q = None
        self._conditions = {}
        self._order_by = None
        self._limit = None

    def __call__(self):
        '''
            Return list of elements
        '''
        return list(self.__iter__())

    def __iter__(self):
        if self._q is None:
            raise StopIteration
        else:
            self.build_query()
            response_elements = execute_sql(self._q)
            if response_elements is None:
                raise StopIteration
            for row in response_elements:
                (*value, ) = row
                value = self.klass._value_parse_to_dict(*value)
                instance = self.klass(**value)
                instance.id = value['id']
                yield instance

    def build_query(self):
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
        except (TypeError, AttributeError):
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
        try:
            (number, ) = execute_sql(sql_query).fetchone()
        except AttributeError:
            return None
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

    def update(self, **kwargs):
        if self.instance:
            execute_sql(self._create_update_sql())
        else:
            execute_sql(self._create_update_sql_table(**kwargs))

    def _create_update_sql_table(self, **kwargs):
        table_name = self.klass.__name__.lower()
        sql_query = 'UPDATE %s SET ' % table_name
        for field, value in kwargs.items():
            if field in self.klass.Fields:
                if not sql_query.endswith('SET '):
                    sql_query += ', '
                sql_query += '%s = \'%s\'' % (field, value)
        return sql_query

    def _create_update_sql(self):
        table_name = self.klass.__name__.lower()
        sql_query = 'UPDATE %s SET ' % table_name
        for i in self.klass.Fields:
            if not sql_query.endswith('SET '):
                sql_query += ', '
            sql_query += '%s = \'%s\'' % (i, getattr(self.instance, i))
        sql_query += ' WHERE id = %i' % self.instance.id
        return sql_query


class Field:

    def __init__(self, blank=True):
        self.blank = blank
        self.instance = None

    def __get__(self, instance, klass):
        return getattr(instance, str(id(self)))

    def __set__(self, instance, value):
        setattr(instance, str(id(self)), value)

    def simple_valid(self):
        def validation(instance):
            value = getattr(instance, str(id(self)))
            if not self.blank and not value:
                return False
            else:
                return True
        return validation


class BasicModel(type):

    def __new__(meta, classname, supers, classdict):
        fields = {}
        for klass in supers:
            fields.update(meta.parse_fields(klass))
        fields.update(meta.parse_dict_for_fields(classdict))
        classdict['Fields'] = tuple(sorted(fields))
        meta.create_validation_for_field(classdict, fields)
        return type.__new__(meta, classname, supers, classdict)

    @classmethod
    def parse_fields(cls, klass):
        '''
            Check if class doesn't have attribute of Fields then add it with id field
        '''
        fields = {}

        for supercls in klass.__bases__:
            fields.update(cls.parse_fields(supercls))
        if klass.__name__ != 'object':
            fields.update(cls.parse_dict_for_fields(klass.__dict__))
        return fields

    @staticmethod
    def parse_dict_for_fields(classdict):
        '''
            Creates list of fields
        '''
        fields = {}
        for attr, value in classdict.items():
            if isinstance(value, Field):
                fields[attr] = value
        return fields

    @staticmethod
    def create_validation_for_field(classdict, fields_dict):
        for field, value in fields_dict.items():
            valid_field_name = 'valid_' + field
            classdict[valid_field_name] = value.simple_valid()


class Model(metaclass=BasicModel):
    id = Field(blank=True)

    def __init__(self, *args, **kwargs):
        '''
            Create object attribute from class Fields
        '''
        self.id = None
        for field in self.__class__.Fields:
            if field != 'id':
                try:
                    setattr(self, field, kwargs[field])
                except KeyError:
                    setattr(self, field, None)

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
        self.objects.update()

    def delete(self):
        self.objects.delete()

    def is_valid(self):
        for field in self.__class__.Fields:
            valid_field_name = 'valid_' + field
            # Execute validation method for all fields
            if not getattr(self, valid_field_name)():
                return False
        return True

    def _fields_values_to_str(self):
        '''
            Prepared tuple in string of object fields
            values in the order of fields
            Fields = ('id', 'list_id', 'name')
            {'name': 'Something', 'list_id': 5}
            > (5, 'Something)
            If only one field then assume it is
            'id' field then should be null for save
        '''
        if len(self.__class__.Fields) == 1:
            return "(NULL)"
        value = []
        for i in self.__class__.Fields:
            value.append(getattr(self, i))
        return (str(tuple(value))).replace('None', 'NULL')

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

    objects = Query()
