![logo](https://github.com/SimonCqk/cannondb/blob/master/logo/cannon.jpg?raw=true)

CannonDB
========
[![Build Status](https://travis-ci.org/SimonCqk/cannondb.svg?branch=master)](https://travis-ci.org/SimonCqk/cannondb) 
![language](https://img.shields.io/badge/language-python-blue.svg)
![license](https://img.shields.io/badge/license-MIT-000000.svg)

**`CannonDB` is a lightweight but powerful key-value database designed for human beings.** 

### Installation

> pip install cannondb

### Features

- maintained by a on-disk B tree, so insert/get/remove is fast enough.
- `str/int/float/dict/list/UUID` types of key/value are supported.
- store data in file defaulted, but store in memory is also supported. 
- flexible parameter settings(db name/ page size/ key size/ value size /cache size) configuration to satisfy your demand.
- use WAL (write-ahead logging) technique to provide strong safety guarantee.

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

##### about auto commit
`commit` means flush and sync your file data with disk. It ensures the durability of 
your data, while it's time consuming to flush and sync.
if you desire for a good performance, turn-off auto commit, and do it manually.
```python
db.set_auto_commit(False)

# commit manually
db.commit()
```
else you'd just ignore it.

##### about checkpoint
WAL(write-ahead logging) pre-write your committed data into WAL file (see as data buffer cache), 
but not real database file, `checkpoint` does the work of `write all your cached data(has been saved properly) before this 
time point into real database file`.

```python
db.checkpoint()
```

##### logging
`cannondb` provides 3 kind of logging mode.

- 'local': logging in local file (log.log)
- 'tcp'/'udp: use TCP/UDP socket to redirect logging to a concrete host 

```python
import cannondb
# use local mode
db = cannondb.connect(log='local')

# use tcp/udp mode
# host and port must be specified.
db = cannondb.connect(log='tcp', host='127.0.0.1', port=2048)
```

###### Do not forget to close db when exit.

### TODO

- refactor some I/O operations into async model (see `async` branch).
- support networking requests(client/server).
