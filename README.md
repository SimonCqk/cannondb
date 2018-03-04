![logo](https://github.com/SimonCqk/cannondb/blob/master/logo/cannon.jpg?raw=true)

CannonDB
========

**`CannonDB` is a lightweight but powerful key-value database designed for human beings.** 

### Features

- maintained by a on-disk B tree, so insert/get/remove is fast enough.
- str/int/float/dict types of key/value are supported.
- storage data in file defaulted, but storage in memory is also supported. 
- flexible parameter (db name/ page size/ key size/ value size /cache size) configuration to satisfy your demand.
- use WAL (write-logging ahead) technique to provide strong safety guarantee.

### Performance

|  Platform  |    CPU   |  Memory  |
| :---------: |:-------:| :------: |
| Windows 10 | i5-5200U |    8G    |


- about write 3000 records per second.
- about read 14000 records per second. 

since my current machine is out-of-date, it'll absolutely run a better 
performance on other machine.  

### How to use

##### create a db instance

```python
import cannondb
# create by call <connect> function
db = cannondb.connect()

# create by instantiate a CannonDB class

db = cannondb.CannonDB()
```

##### insert key value pairs
```python
import cannondb
db = cannondb.connect()

# kinds of type-combination as your pleasant
db.insert('1234',4321)
db.insert('test','today')
db.insert('pi',3.1415926)
db.insert('dict',{'a':1,'b':2})

assert db.get('1234') == 4321
assert db.get('test') == 'today'
assert db['pi'] == 3.1415926
assert db['dict'] == {'a':1,'b':2}

db.close()
```
if a key-value has existed, when you want to override it, use
it like this, or a KeyError will be raised.
```python
db.insert('1234',1234,override=True)

assert db['1234'] == 1234
```

##### remove a key-value pair
```python
db.remove('test')

# or

del db['pi']
```

###### Do not forget to close db when exit.

### TODO

- refactor all I/O operations into `async` model.
- complete wrappers to enhance functions of database.