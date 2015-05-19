# -*- coding: utf-8 -*-

import MySQLdb
from datetime import datetime
import json

con_params = {
    'db': '',
    'host': 'localhost',
    'user': '',
    'passwd': ''
}


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

    '''Instance builds query for the databases

    Attributes:
      instance : Instance of model.
      klass: Class of model.
      _q (str): Query for database.
      _conditions (dict): All conditions from filter.
      _order_by (str): Description of order how returns list of instance.
      _limit (str): Simple MySQL limit statement.
    '''

    def __init__(self, instance, klass):
        self.instance = instance
        self.klass = klass
        self._q = None
        self._conditions = {}
        self._order_by = None
        self._limit = None

    def __call__(self):
        '''Returns list of model instance.'''

        return list(self.__iter__())

    def __iter__(self):
        if self._q is None:
            raise StopIteration
        else:
            self._build_query()
            response_elements = execute_sql(self._q)
            if response_elements is None:
                raise StopIteration
            for row in response_elements:
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

    def _build_query(self):
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

    def create(self, raw_json=None, **kwargs):
        '''Saves to databases and returns instance of model.

        Args:
          raw_json (json, optional): Json data for create model. Defaults to None.
          kwargs: Kwargs for create model. This same like fields in model.

        Returns:
          Instance of model.
        '''
        if raw_json is not None:
            kwargs_from_json = json.loads(raw_json)
            kwargs.update(kwargs_from_json)
        instance = self.klass(**kwargs)
        if instance.is_valid():
            instance.save()
        return instance

    def delete(self, id=None):
        '''Delete model from databases.

        Args:
          id: Id of model which should be remove.
        '''
        if id is not None or self.instance.id:
            table_name = self.klass.__name__.lower()
            sql = 'DELETE FROM %s WHERE id = %s' % (table_name,
                                                    id or self.instance.id)
            execute_sql(sql)

    def get_or_create(self, raw_json=None, **kwargs):
        '''Gets or creates model and returns instance.

        Note:
          raw_json and kwargs are combined.

        Args:
          id (int, optional): Id of model in databases.
          raw_json (json, optional): Json data for create model. Defaults to None.
          kwargs: Kwargs for create model. This same like fields in model.
        '''
        if raw_json is not None:
            kwargs_from_json = json.loads(raw_json)
            kwargs.update(kwargs_from_json)
        instance = None
        if kwargs.get('id', None):
            instance = self.get(id=kwargs['id'])
        # instance can be empty
        if not instance:
            instance = self.create(**kwargs)
        return instance

    def get(self, resp_json=False, **kwargs):
        '''Returns instance of Model.

        Args:
          id (int): Id of model in databases.
          resp_json (bool): If true then returns json, otherwise instance.

        Returns:
          If exist then return instance of model or json.
        '''
        sql_query = self.klass._simple_query()
        sql_query += self._parse_conditions_to_sql(**kwargs)
        try:
            (*value, ) = execute_sql(sql_query).fetchone()
        except (TypeError, AttributeError):
            return None
        value = self.klass._value_parse_to_dict(*value)
        if resp_json:
            return json.dumps(value, default=json_serial)
        instance = self.klass(**value)
        instance.id = value['id']
        return instance

    def all(self):
        '''Prepares query for returns all instance from databases

        Returns:
          Instance of Query.
        '''
        self._q = self.klass._simple_query()
        return self

    def filter(self, **kwargs):
        '''Build dict of conditions

        Args:
          kwargs: This same name like fields in model and value for condition.

        Returns:
          Instance of Query.
        '''
        self._q = self.klass._simple_query()
        self._conditions.update(kwargs)
        return self

    def order_by(self, *args):
        '''Prepares query for returns instances in specific orders

        Args:
          args (string): This same name like fields in model.

        Returns:
          Instance of Query.

        Examples:
          Model.objects.all().order_by('id') # ASC
          Model.objects.all().order_by('-id') # DESC
        '''
        sql_query = 'ORDER BY '
        if not args:
            self._order_by = sql_query + 'id ASC'
            return self
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
        '''Returns number model records in databases'''

        table_name = self.klass.__name__.lower()
        sql_query = 'SELECT COUNT(*) FROM %s' % table_name
        try:
            (number, ) = execute_sql(sql_query).fetchone()
        except AttributeError:
            return None
        return number

    def execute_query(self, query):
        '''Execute query for databases and returns list of instance

        Args:
          query (string): Query to databases.

        Returns:
          List of models instance from result of query.
        '''
        self._q = query
        return list(self)

    def json(self):
        '''Returns result of query in json.'''

        instances_list = []
        for instance in list(self.__iter__()):
            instance_to_dict = {}
            for field in self.klass.Fields:
                instance_to_dict[field] = getattr(instance, field)
            instances_list.append(instance_to_dict)
        return json.dumps(instances_list, default=json_serial)

    def _parse_conditions_to_sql(self, **kwargs):
        sql_query = ' WHERE '
        for key, value in kwargs.items():
            if not sql_query.endswith('WHERE '):
                sql_query += ' AND '
            key, sign = self._parse_to_sign(key)
            sql_query += '%s %s \'%s\'' % (key, sign, value)
        return sql_query

    def _parse_to_sign(self, key):
        signs = {'': '=', 'lt': '<', 'lte': '<=',
                 'gt': '>', 'gte': '>=', 'like': 'like'}
        for field in self.klass.Fields:
            for sign in signs:
                if sign == '':
                    underscore = ''
                else:
                    underscore = '__'
                temp_key = field + underscore + sign
                if temp_key == key:
                    return field, signs[sign]

    def update(self, raw_json=None, resp_json=False, **kwargs):
        '''Updates a record from kwargs or from json
        and returns instance of model or json.

        Args:
          raw_json (json, optional): Json data for update model. Default None.
          resp_json (json, optional): If true then returns json, otherwise instance.
          kwargs: This same name like fields in model with value for updates.
        '''
        if self.instance:
            execute_sql(self._create_update_sql())
        else:
            if raw_json is not None:
                kwargs_from_json = json.loads(raw_json)
                kwargs.update(kwargs_from_json)
            execute_sql(self._create_update_sql_from_kwargs(**kwargs))
            if kwargs.get('id', None):
                if resp_json:
                    return self.get(id=kwargs['id'], resp_json=True)
                return self.get(id=kwargs['id'])

    def _create_update_sql_from_kwargs(self, **kwargs):
        table_name = self.klass.__name__.lower()
        sql_query = 'UPDATE %s SET ' % table_name
        for field, value in kwargs.items():
            if field in self.klass.Fields:
                if not sql_query.endswith('SET '):
                    sql_query += ', '
                sql_query += '%s = \'%s\'' % (field, value)
        if kwargs.get('id', None):
            sql_query += ' WHERE id = %i' % kwargs['id']
        return sql_query

    def _create_update_sql(self):
        '''
            Create query SQL when exist instance of Model
        '''
        table_name = self.klass.__name__.lower()
        sql_query = 'UPDATE %s SET ' % table_name
        for i in self.klass.Fields:
            if not sql_query.endswith('SET '):
                sql_query += ', '
            sql_query += '%s = \'%s\'' % (i, getattr(self.instance, i))
        sql_query += ' WHERE id = %i' % self.instance.id
        return sql_query


class Field:

    '''Represents field in database.

    Attributes:
        primary_key (bool, optional): Describes whether field is primary key.
        null (bool, optional): Describes whether field can be null in databases.
        blank (bool, optional): Describes whether field can be blank.
        default (optional): Default value which will be used for save to databases.
    '''

    def __init__(self, primary_key=False, null=True, blank=True, default=None):
        self.primary_key = primary_key
        self.null = null
        self.blank = blank
        self.default = default

    def __get__(self, instance, klass):
        return getattr(instance, str(id(self)))

    def __set__(self, instance, value):
        setattr(instance, str(id(self)), value)

    def simple_valid(self):
        def validation(instance):
            value = getattr(instance, str(id(self)))
            if self.blank is False and not value:
                if self.default is not None:
                    return True
                else:
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
        # Removes from fields alias pk
        fields.pop('pk', None)
        classdict['Fields'] = tuple(sorted(fields))
        meta.create_validation_for_field(classdict, fields)
        return type.__new__(meta, classname, supers, classdict)

    @classmethod
    def parse_fields(cls, klass):
        '''Moves through in all bases of classes and builds dict of fields'''

        fields = {}

        for supercls in klass.__bases__:
            fields.update(cls.parse_fields(supercls))
        if klass.__name__ != 'object':
            fields.update(cls.parse_dict_for_fields(klass.__dict__))
        return fields

    @staticmethod
    def parse_dict_for_fields(classdict):
        '''Creates dict of fields and instance Field object'''

        fields = {}
        for attr, value in classdict.items():
            if isinstance(value, Field):
                fields[attr] = value
        return fields

    @staticmethod
    def create_validation_for_field(classdict, fields_dict):
        '''Creating aliases for simple validations of fields'''

        for field, value in fields_dict.items():
            valid_field_name = 'valid_' + field
            # Check if field is primary key then creates alias with name pk
            if value.primary_key:
                classdict['pk'] = value
            classdict[valid_field_name] = value.simple_valid()


class Model(metaclass=BasicModel):
    id = Field(primary_key=True, blank=True)

    def __init__(self, *args, **kwargs):
        ''' Create object attribute from class attribute of Fields'''
        self.id = None
        for field in self.__class__.Fields:
            if field != 'id':
                try:
                    setattr(self, field, kwargs[field])
                except KeyError:
                    setattr(self, field, None)

    def __str__(self):
        return 'Object'

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.__str__())

    def save(self):
        '''
            Saved is only if doesn't has id, else run update
        '''
        if self.id is None:
            table_name = self.__class__.__name__.lower()
            sql_query = '''INSERT INTO %s (%s) values%s ''' % (
                table_name, self._parse_fields(), self._fields_values_to_str())
            cursor = execute_sql(sql_query)
            if cursor is not None:
                self.id = cursor.lastrowid
        else:
            self.update()
        return self

    def update(self):
        '''Update record databases of current instance'''

        self.objects.update()

    def delete(self):
        '''Removes current object from databases'''

        self.objects.delete()

    def is_valid(self):
        '''Checks all fields for error.

        Executing methods of validation for all fields.

        Returns:
          True if all fields are valid, otherwise False.
        '''
        for field in self.__class__.Fields:
            # Preparing names of validation methods for field
            valid_field_name = 'valid_' + field
            if getattr(self, valid_field_name)() is False:
                return False
        return True

    def _fields_values_to_str(self):
        '''Parse fields values into string.

        Prepared tuple in string of object fields
        values in the order of fields.

        Note:
          If only one field then assume it is 'id'
          field then should be null for save.

        Returns:
          String of fields value.

        Examples:
          Fields = ('id', 'list_id', 'name')
          {'name': 'Something', 'list_id': 5}
          (5, 'Something)
        '''
        if len(self.__class__.Fields) == 1:
            return "(NULL)"
        value = []
        for i in self.__class__.Fields:
            value.append(getattr(self, i))
        return (str(tuple(value))).replace('None', 'NULL')

    @classmethod
    def _value_parse_to_dict(cls, *value):
        '''Combines correct of value with fields and return dict

        Args:
          value (tuple): Value which are fetched from database.

        Returns:
          Returns dict of fields with value.
        '''
        dict_values = {}
        for field, value in zip(cls.Fields, value):
            dict_values[field] = value
        return dict_values

    @classmethod
    def _simple_query(cls):
        '''Simple SQL query with names of fields and table name'''

        return 'SELECT %s FROM %s' % (cls._parse_fields(), cls.__name__.lower())

    @classmethod
    def _parse_fields(cls):
        '''Parse model fields into string.

        Returns:
          Returns string of fields name.

        Examples:
          Fields = ('id', 'list_id', 'name')
          tuple_of_fields = 'id, list_id, name'
        '''
        tuple_of_fields = ''
        for key in cls.Fields:
            if tuple_of_fields != '':
                tuple_of_fields += ', '
            tuple_of_fields += key
        return tuple_of_fields

    objects = Query()

# Helpers


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


def json_serial(obj):
    '''JSON serializer for objects not serializable by default json code'''

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")
