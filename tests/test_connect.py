import cannondb

db = cannondb.connect('test_db')

db.insert('test', 1)

db.close()
