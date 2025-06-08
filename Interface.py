#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import io

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load data from ratingsfilepath into ratingstablename efficiently.
    """
    con = openconnection
    cur = con.cursor()
    # Tạo bảng đúng schema
    cur.execute("DROP TABLE IF EXISTS %s;" % ratingstablename)
    cur.execute("CREATE TABLE %s (userid INTEGER, movieid INTEGER, rating FLOAT);" % ratingstablename)

    # Đọc file, chỉ lấy 3 trường cần thiết, ghi vào buffer
    buffer = io.StringIO()
    with open(ratingsfilepath, 'r') as f:
        for line in f:
            fields = line.strip().split(':')
            if len(fields) >= 5:
                buffer.write("%s\t%s\t%s\n" % (fields[0], fields[2], fields[4]))
    buffer.seek(0)

    # Nạp dữ liệu vào bảng
    cur.copy_from(buffer, ratingstablename, sep='\t', columns=('userid', 'movieid', 'rating'))
    buffer.close()
    cur.close()
    con.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions  # float division
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(numberofpartitions):
        minRange = i * delta
        # Partition cuối cùng lấy hết đến 5.0
        maxRange = 5.0 if i == numberofpartitions - 1 else (i + 1) * delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS %s;" % table_name)
        cur.execute("CREATE TABLE %s (userid INTEGER, movieid INTEGER, rating FLOAT);" % table_name)
        if i == 0:
            cur.execute(
                "INSERT INTO %s (userid, movieid, rating) "
                "SELECT userid, movieid, rating FROM %s WHERE rating >= %%s AND rating <= %%s;" % (table_name, ratingstablename),
                (minRange, maxRange)
            )
        else:
            cur.execute(
                "INSERT INTO %s (userid, movieid, rating) "
                "SELECT userid, movieid, rating FROM %s WHERE rating > %%s AND rating <= %%s;" % (table_name, ratingstablename),
                (minRange, maxRange)
            )
    cur.close()
    con.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Tạo các bảng phân mảnh (nếu chưa tồn tại)
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("CREATE TABLE IF NOT EXISTS %s (userid INTEGER, movieid INTEGER, rating FLOAT);" % table_name)

    # Tạo bảng tạm với row_number để chỉ tính 1 lần
    cur.execute("""
        CREATE TEMP TABLE temp_rr AS
        SELECT userid, movieid, rating, (ROW_NUMBER() OVER() - 1) AS rnum
        FROM %s;
    """ % ratingstablename)

    # Chèn dữ liệu vào từng partition
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(
            "INSERT INTO %s (userid, movieid, rating) "
            "SELECT userid, movieid, rating FROM temp_rr WHERE MOD(rnum, %%s) = %%s;" % table_name,
            (numberofpartitions, i)
        )

    # Xóa bảng tạm
    cur.execute("DROP TABLE temp_rr;")
    cur.close()
    con.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert a new row into the main table and the correct round robin partition.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    # Tham số hóa truy vấn
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%%s, %%s, %%s);" % ratingstablename, (userid, itemid, rating))
    cur.execute("SELECT COUNT(*) FROM %s;" % ratingstablename)
    total_rows = cur.fetchone()[0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    if numberofpartitions == 0:
        cur.close()
        con.commit()
        return
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%%s, %%s, %%s);" % table_name, (userid, itemid, rating))
    cur.close()
    con.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert a new row into the main table and the correct range partition.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    if numberofpartitions == 0:
        cur.close()
        con.commit()
        return
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    if index >= numberofpartitions:
        index = numberofpartitions - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%%s, %%s, %%s);" % table_name, (userid, itemid, rating))
    cur.close()
    con.commit()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count
