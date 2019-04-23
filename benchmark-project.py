#!/usr/bin/python
import psycopg2
import random
import sys
con = None

#convert the stringu1 and stringu2 to have first 7 characters between 'A-Z' followed by 45 x's
def char_convert(unique):
    result = []
    u = unique
    tmp = ['A', 'A', 'A', 'A', 'A', 'A', 'A']
    for i in range(52):
        if i < 7:
            result.append('A')
        else:
            result.append('x')
    i = 6
    while u > 0:
        rem = u % 26
        tmp[i] = (chr(ord('A') + rem))
        u = int(u/26)
        i = i - 1
    j = 1
    for j in range(7):
        result[j] = tmp[j]

    n = ''
    for x in result:
        n += x

    return n

#cyclic generation of the string4 tuple to have AAAAxxxx's, HHHHxxx's, OOOOxxxx's, VVVVxxxx's
def generate_string4(tupCount):
    result = list()
    u = tupCount
    n = ''
    for i in range(52):
        result.append('x')
    if u % 4 == 0:
        for j in range(4):
            result[j] = 'A'
    if u % 4 == 1:
        for j in range(4):
            result[j] = 'H'
    if u % 4 == 2:
        for j in range(4):
            result[j] = 'O'
    if u % 4 == 3:
        for j in range(4):
            result[j] = 'V'
#convert the list to string
    for x in result:
        n += x
    return n


#generate the remaining tuples using the unique1 value and insert into sql table
def generate_relation(tupCount, filename):
    print(tupCount)
    unique2 = list(range(0, tupCount))
    unique1 = random.sample(unique2, tupCount)
    for i in range(tupCount):
        two = random.choice(unique1) % 2
        four = random.choice(unique1) % 4
        ten = random.choice(unique1) % 10
        twenty = random.choice(unique1) % 20
        onePercent = random.choice(unique1) % 100
        tenPercent = random.choice(unique1) % 10
        twentyPercent = random.choice(unique1) % 5
        fiftyPercent = random.choice(unique1) % 2
        unique3 = random.choice(unique1)
        evenOnePercent = onePercent * 2
        oddOnePercent = (onePercent * 2) + 1
        stringu1 = char_convert(unique1[i])
        stringu2 = char_convert(unique2[i])
        string4 = generate_string4(i)
        #use cursor the insert data into table
        if tupCount == 1000 and filename == 'onektup':
            cur.execute("INSERT INTO ONEKTUP (unique1, unique2, two,four,ten,twenty,"
                        "onePercent,tenPercent,twentyPercent,"
                        "fiftyPercent,unique3,evenOnePercent,oddOnePercent,stringu1, stringu2, string4)"
                        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (unique1[i], unique2[i], two, four, ten, twenty, onePercent, tenPercent, twentyPercent,
                         fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4))
        elif tupCount == 10000 and filename == 'tenktup1':
            cur.execute("INSERT INTO TENKTUP1 (unique1, unique2, two,four,ten,twenty,"
                        "onePercent,tenPercent,twentyPercent,"
                        "fiftyPercent,unique3,evenOnePercent,oddOnePercent,stringu1, stringu2, string4)"
                        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (unique1[i], unique2[i], two, four, ten, twenty, onePercent, tenPercent, twentyPercent,
                         fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4))

        elif tupCount == 10000 and filename == 'tenktup2':
            cur.execute("INSERT INTO TENKTUP2 (unique1, unique2, two,four,ten,twenty,"
                        "onePercent,tenPercent,twentyPercent,"
                        "fiftyPercent,unique3,evenOnePercent,oddOnePercent,stringu1, stringu2, string4)"
                        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (unique1[i], unique2[i], two, four, ten, twenty, onePercent, tenPercent, twentyPercent,
                         fiftyPercent, unique3, evenOnePercent, oddOnePercent, stringu1, stringu2, string4))

#connect to database if the connection is successful create table and insert records into the table
try:
    tupCount = int(sys.argv[1])
    #connect to PostgreSQL database
    con = psycopg2.connect("host='localhost' dbname='testdb' user='postgres' password='onelove'")
    #create a new cursor
    cur = con.cursor()
    #execute the insert statement
    if tupCount == 1000:
        cur.execute("CREATE TABLE ONEKTUP(unique1 INTEGER UNIQUE NOT NULL, "
                    "unique2 INTEGER PRIMARY KEY,"
                    "two INTEGER NOT NULL,"
                    "four INTEGER NOT NULL,"
                    "ten INTEGER NOT NULL,"
                    "twenty INTEGER NOT NULL,"
                    "onePercent INTEGER NOT NULL,"
                    "tenPercent INTEGER NOT NULL,"
                    "twentyPercent INTEGER NOT NULL,"
                    "fiftyPercent INTEGER NOT NULL,"
                    "unique3 INTEGER NOT NULL,"
                    "evenOnePercent INTEGER NOT NULL,"
                    "oddOnePercent INTEGER NOT NULL,"
                    "stringu1 CHAR(52) UNIQUE NOT NULL,"
                    "stringu2 CHAR(52) UNIQUE NOT NULL,"
                    "string4 CHAR(52) NOT NULL)")
        filename = 'onektup'
        #generate the data using the algorithm used in wisconsin benchmark
        generate_relation(tupCount,filename)

    elif tupCount == 10000:
        cur.execute("CREATE TABLE TENKTUP1(unique1 INTEGER UNIQUE NOT NULL, "
                    "unique2 INTEGER PRIMARY KEY,"
                    "two INTEGER NOT NULL,"
                    "four INTEGER NOT NULL,"
                    "ten INTEGER NOT NULL,"
                    "twenty INTEGER NOT NULL,"
                    "onePercent INTEGER NOT NULL,"
                    "tenPercent INTEGER NOT NULL,"
                    "twentyPercent INTEGER NOT NULL,"
                    "fiftyPercent INTEGER NOT NULL,"
                    "unique3 INTEGER NOT NULL,"
                    "evenOnePercent INTEGER NOT NULL,"
                    "oddOnePercent INTEGER NOT NULL,"
                    "stringu1 CHAR(52) UNIQUE NOT NULL,"
                    "stringu2 CHAR(52) UNIQUE NOT NULL,"
                    "string4 CHAR(52) NOT NULL)")
        filename = 'tenktup1'
        generate_relation(tupCount, filename)

        cur.execute("CREATE TABLE TENKTUP2(unique1 INTEGER UNIQUE NOT NULL, "
                    "unique2 INTEGER PRIMARY KEY,"
                    "two INTEGER NOT NULL,"
                    "four INTEGER NOT NULL,"
                    "ten INTEGER NOT NULL,"
                    "twenty INTEGER NOT NULL,"
                    "onePercent INTEGER NOT NULL,"
                    "tenPercent INTEGER NOT NULL,"
                    "twentyPercent INTEGER NOT NULL,"
                    "fiftyPercent INTEGER NOT NULL,"
                    "unique3 INTEGER NOT NULL,"
                    "evenOnePercent INTEGER NOT NULL,"
                    "oddOnePercent INTEGER NOT NULL,"
                    "stringu1 CHAR(52) UNIQUE NOT NULL,"
                    "stringu2 CHAR(52) UNIQUE NOT NULL,"
                    "string4 CHAR(52) NOT NULL)")
        filename = 'tenktup2'
        generate_relation(tupCount, filename)
    else:
        print('Wrong number of rows requested')
        sys.exit(1)
    #commit the changes to database
    con.commit()
except psycopg2.DatabaseError as e:
    if con:
        con.rollback()
    print("Error %s" % e)
    sys.exit(1)
finally:
    if con:
        con.close()

