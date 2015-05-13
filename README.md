#Simple MySQL ORM
This project is only developed for improved understanding Python by me.

Connection parameters in file db.py
```python
con_params = {
    'db': '',
    'host': 'localhost',
    'user': '',
    'passwd': ''
}
```
###Example how use it:

Create class with fields

```python
class EventModel(Model):
    text = Field(blank=False)
    category = Field(blank=True)
    person = Field(blank=False)
    date = Field(blank=False)
```
Then you should create table with those same fields and add id field:
```sql
CREATE TABLE eventmodel 
(
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    text TEXT NOT NULL,
    category CHAR(7),
    person char(12) NOT NULL,
    date DATETIME NOT NULL
);
```

```python
>>> EventModel.objects.create(text='Beer break', person='@all', date='150513')
>>> EventModel.objects.all()
[<model.EventModel object at 0x104063240>]
# Query object provides serializer json 
>>> EventModel.objects.all().json()
'[{"id": 1, "category": null, "text": "Beer break", "person": "@all", "date": "2015-05-13T00:00:00"}]'
```