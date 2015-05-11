# -*- coding: utf-8 -*-
import pytest
from inspect import ismethoddescriptor
import db
from db import Model, Field


# Fixtures for Model


@pytest.yield_fixture(scope='function')
def instance_model():
    yield Model().save()
    # database.execute_sql('TRUNCATE model')


@pytest.yield_fixture(scope='function')
def list_models():
    yield [Model().save(), Model().save(), Model().save(), Model().save()]
    # database.execute_sql('TRUNCATE model')


class TestConnect:

    def test_connect(self):
        assert db.connect()


class TestSQLQuery:

    def test_all_model(self):
        query = Model.objects.all()
        assert query._q == 'SELECT id FROM model'

    def test_all_model_with_limit(self):
        query = Model.objects.all()[3]
        assert query._q == 'SELECT id FROM model'
        assert query._limit == 'LIMIT 3 '

    def test_all_model_with_advenced_limit(self):
        query = Model.objects.all()[3:7]
        assert query._q == 'SELECT id FROM model'
        assert query._limit == 'LIMIT 3, 4 '

    def test_filter_model(self):
        query = Model.objects.filter(id=5)
        assert query._q == 'SELECT id FROM model'
        assert query._conditions == {'id': 5}

    def test_filter_with_limit(self):
        query = Model.objects.filter(id=5)[9]
        assert query._q == 'SELECT id FROM model'
        assert query._conditions == {'id': 5}
        assert query._limit == 'LIMIT 9 '

    def test_fluent_filter_model(self):
        query = Model.objects.filter(id=5).filter(list_id=11)
        assert query._q == 'SELECT id FROM model'
        assert query._conditions == {'id': 5, 'list_id': 11}

    def test_fluent_all_model_order_by_and_advenced_limit(self):
        query = Model.objects.all().order_by('-id')[3:7]
        assert query._q == 'SELECT id FROM model'
        assert query._limit == 'LIMIT 3, 4 '
        assert query._order_by == 'ORDER BY id DESC'

    def test_kwargs_to_sql_query_parse(self):
        sql_query = Model.objects._parse_conditions_to_sql(id=1, list_id=3)
        assert sql_query in (
            ' WHERE list_id = \'3\' AND id = \'1\'',
            ' WHERE id = \'1\' AND list_id = \'3\'')

    def test_create_update_sql(self):
        mock_instance = HelperModel(name='Something to do', list_id=1)
        mock_instance.id = 5
        sql_query = mock_instance.objects._create_update_sql()
        assert sql_query == "UPDATE helpermodel SET id = '5', list_id = '1', name = 'Something to do' WHERE id = 5"

    def test_create_update_sql_table(self):
        sql_query = HelperModel.objects._create_update_sql_table(name="Beer")
        assert sql_query == "UPDATE helpermodel SET name = 'Beer'"

    def test_value_parse_to_dict(self):
        dict_value = HelperModel._value_parse_to_dict(1, 6, 'Buy new computer')
        assert dict_value == {
            'name': 'Buy new computer', 'list_id': 6, 'id': 1}


class TestForUniqueQuery:

    @classmethod
    def setup_class(cls):
        db.execute_sql(
            'CREATE TABLE model (id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY)')

    @classmethod
    def teardown_class(cls):
        db.execute_sql('DROP TABLE model')

    def teardown(self):
        db.execute_sql('TRUNCATE model')

    def test_unique_query_from_class(self):
        query_1 = Model.objects.all()
        query_2 = Model.objects.all()
        assert id(query_1) != id(query_2)

    def test_unique_query_from_instance(self, instance_model):
        query_1 = instance_model.objects.all()
        query_2 = instance_model.objects.all()
        assert id(query_1) != id(query_2)


class TestModel:

    @classmethod
    def setup_class(cls):
        db.execute_sql(
            'CREATE TABLE model (id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY)')

    @classmethod
    def teardown_class(cls):
        db.execute_sql('DROP TABLE model')

    def teardown(self):
        db.execute_sql('TRUNCATE model')

    def test_add_id_fields(self):
        assert hasattr(Model, 'Fields')
        assert 'id' in Model.Fields

    def test_one_field_values_to_str(self):
        instance = Model()
        assert instance._fields_values_to_str(
        ) == "(NULL)"

    def test_count_model(self):
        assert Model.objects.count() == 0

    def test_save_model(self):
        Model().save()
        assert Model.objects.count() == 1

    def test_read_model(self, instance_model):
        model = Model.objects.get(id=instance_model.id)
        assert model.id

    def test_delete_model(self, instance_model):
        instance_model.delete()
        assert Model.objects.count() == 0

    def test_delete_model_by_id(self, instance_model):
        Model.objects.delete(id=instance_model.id)
        assert Model.objects.count() == 0

    def test_create_model(self):
        Model.objects.create()
        assert Model.objects.count() == 1

    def test_all_empty_list_models(self):
        model = Model.objects.all()
        assert list(model) == []

    def test_all_list_with_one_model(self, instance_model):
        model = Model.objects.all()[1]
        assert len(model) == 1

    def test_all_list(self, list_models):
        models = Model.objects.all()
        assert len(models) == 4


class HelperModel(Model):

    '''
        Helper model for more advanced tests
    '''
    # Fields = ('id', 'list_id', 'name')
    list_id = Field(blank=False)
    name = Field(blank=False)

    @staticmethod
    def create_table_for_test():
        db.execute_sql('''
        CREATE TABLE helpermodel(
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            list_id  INT UNSIGNED NOT NULL,
            name CHAR(60) NOT NULL
        )
        ''')

    @staticmethod
    def drop_table():
        db.execute_sql('DROP TABLE helpermodel')


# Fixtures for HelperModel
@pytest.yield_fixture(scope='function')
def instance_helpermodel():
    yield HelperModel(name='Something to do', list_id=1).save()


@pytest.yield_fixture(scope='function')
def list_helpermodel():
    yield [HelperModel(name='Something to do', list_id=1).save(),
           HelperModel(name='Read a book', list_id=2).save(),
           HelperModel(name='Buy carrot', list_id=2).save(),
           HelperModel(name='Read a book', list_id=1).save()
           ]


class TestHelperModel:

    @classmethod
    def setup_class(cls):
        HelperModel.create_table_for_test()

    @classmethod
    def teardown_class(cls):
        HelperModel.drop_table()

    def teardown(self):
        db.execute_sql('TRUNCATE helpermodel')

    def test_parse_fields(self):
        assert HelperModel._parse_fields() == 'id, list_id, name'

    def test_simple_query(self):
        assert HelperModel._simple_query(
        ) == 'SELECT id, list_id, name FROM helpermodel'

    def test_initializer(self):
        instance = HelperModel(id=5, list_id=7, name='Help', nothing=54)
        assert instance.list_id == 7
        assert instance.name == 'Help'
        assert instance.id is None
        assert hasattr(instance, 'nothing') is False

    def test_fields_values_to_str(self):
        instance = HelperModel()
        instance.name = 'Something'
        assert instance._fields_values_to_str(
        ) == "(NULL, NULL, 'Something')"

    def test_count_helpermodels(self):
        assert HelperModel.objects.count() == 0

    def test_save_heplermodel(self):
        HelperModel(name='Something', list_id=7).save()
        assert HelperModel.objects.count() == 1

    def test_read_helpermodel(self, instance_helpermodel):
        instance = HelperModel.objects.get(id=instance_helpermodel.id)
        assert instance.name == 'Something to do'

    def test_update_helpermodel(self, instance_helpermodel):
        instance_helpermodel.name = 'Fly like cat'
        instance_id = instance_helpermodel.id
        instance_helpermodel.update()
        instance = HelperModel.objects.get(id=instance_id)
        assert instance.name == 'Fly like cat'

    def test_update_without_instance(self, list_helpermodel):
        HelperModel.objects.update(name='Beer')
        instances = HelperModel.objects.filter(name='Beer')
        assert len(instances) == 4

    # Delete instance and delete by id are accurate in TestModel

    def test_create_helpermodel(self):
        instance_id = HelperModel.objects.create(name='Cat', list_id=7).id
        instance = HelperModel.objects.get(id=instance_id)
        assert instance.name == 'Cat'

    def test_get_or_create_helpermodel(self):
        HelperModel.objects.get_or_create(name='Inbox', id=5, list_id=6)
        assert HelperModel.objects.count() == 1

    def test_get_or_create_helpermodel_if_exist(self, list_helpermodel):
        instance_id = list_helpermodel[2].id
        instance = HelperModel.objects.get_or_create(
            name='Shower', id=instance_id, list_id=6)
        assert instance.name == 'Buy carrot'
        assert HelperModel.objects.count() == 4

    def test_all_with_advenced_limit(self, list_helpermodel):
        instances = HelperModel.objects.all()[2:4]
        instances = list(instances)
        assert len(instances) == 2
        assert instances[1].name == 'Read a book'
        assert instances[0].name == 'Buy carrot'

    def test_filter_by_name(self, list_helpermodel):
        instances = HelperModel.objects.filter(name='Read a book')
        assert len(instances) == 2

    def test_fluent_filter(self, list_helpermodel):
        instances = HelperModel.objects.filter(
            name='Read a book').filter(list_id=2)
        assert len(instances) == 1

    def test_fluent_filter_with_limit(self, list_helpermodel):
        instances = HelperModel.objects.filter(list_id=2)[1:2]
        assert len(instances) == 1
        assert list(instances)[0].name == 'Buy carrot'

    def test_order_by(self, list_helpermodel):
        instances = HelperModel.objects.all().order_by('-id')
        assert list(instances)[0].name == list_helpermodel[-1].name

    def test_order_with_filter(self, list_helpermodel):
        instances = HelperModel.objects.filter(list_id=2).order_by('-id')
        assert list(instances)[0].name == 'Buy carrot'

    def test_execute_query(self, list_helpermodel):
        query = 'SELECT * FROM helpermodel where id > 2'
        instances = HelperModel.objects.execute_query(query)
        assert len(instances) == 2

    def test_execute_query_two(self, list_helpermodel):
        query = 'SELECT * FROM helpermodel where id > 2 and list_id = 1'
        instances = HelperModel.objects.execute_query(query)
        assert len(instances) == 1

    def test_fiedls_have_validation(self, instance_helpermodel):
        assert hasattr(instance_helpermodel, 'valid_name')
        ismethoddescriptor(getattr(instance_helpermodel, 'valid_name'))

    def test_validation_work_correctly(self):
        instance = HelperModel(name='Beer', list_id=5)
        assert instance.is_valid() is True

    def test_validation_work_correctly_with_bad_data(self):
        instance = HelperModel(list_id=5)
        assert instance.is_valid() is False
