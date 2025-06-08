#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import io

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def create_metadata_table(openconnection):
    """
    Tạo bảng metadata để lưu trữ thông tin về các phân mảnh
    """
    cur = openconnection.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partition_metadata (
            partition_type VARCHAR(10) NOT NULL,
            partition_count INTEGER NOT NULL,
            total_rows INTEGER NOT NULL DEFAULT 0,
            last_insert_idx INTEGER DEFAULT 0,
            PRIMARY KEY (partition_type)
        );
    """)
    
    openconnection.commit()
    cur.close()


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
    Tạo phân mảnh dựa trên khoảng giá trị rating và cập nhật metadata
    """
    cur = openconnection.cursor()
    
    # Tạo bảng metadata nếu chưa tồn tại
    create_metadata_table(openconnection)
    
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    # Tạo các phân mảnh mới
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = 5.0 if i == numberofpartitions - 1 else (i + 1) * delta
        table_name = RANGE_TABLE_PREFIX + str(i)

        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        if i == 0:
            cur.execute(
                f"INSERT INTO {table_name} (userid, movieid, rating) "
                f"SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating >= %s AND rating <= %s;",
                (minRange, maxRange)
            )
        else:
            cur.execute(
                f"INSERT INTO {table_name} (userid, movieid, rating) "
                f"SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating > %s AND rating <= %s;",
                (minRange, maxRange)
            )
    
    # Lấy tổng số bản ghi và lưu vào metadata
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    
    # Cập nhật metadata
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, total_rows)
        VALUES ('range', %s, %s);
    """, (numberofpartitions, total_rows))
    
    openconnection.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo phân mảnh theo kiểu round robin và cập nhật metadata
    """
    cur = openconnection.cursor()
    
    # Tạo bảng metadata nếu chưa tồn tại
    create_metadata_table(openconnection)
    
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Tạo các bảng phân mảnh
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")

    # Sử dụng bảng tạm với row_number
    cur.execute(f"""
        CREATE TEMP TABLE temp_rr AS
        SELECT userid, movieid, rating, (ROW_NUMBER() OVER() - 1) AS rnum
        FROM {ratingstablename};
    """)

    # Chèn dữ liệu vào từng partition
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(
            f"INSERT INTO {table_name} (userid, movieid, rating) "
            f"SELECT userid, movieid, rating FROM temp_rr WHERE MOD(rnum, %s) = %s;",
            (numberofpartitions, i)
        )

    # Xóa bảng tạm
    cur.execute("DROP TABLE temp_rr;")
    
    # Lấy tổng số bản ghi và lưu vào metadata
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    last_idx = (total_rows - 1) % numberofpartitions if total_rows > 0 else 0
    
    # Cập nhật metadata
    cur.execute("""
        INSERT INTO partition_metadata (partition_type, partition_count, total_rows, last_insert_idx)
        VALUES ('rrobin', %s, %s, %s);
    """, (numberofpartitions, total_rows, last_idx))
    
    openconnection.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn dữ liệu vào bảng chính và phân mảnh theo round robin, dùng metadata
    """
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Lấy thông tin từ metadata
    cur.execute("""
        SELECT partition_count, total_rows, last_insert_idx
        FROM partition_metadata
        WHERE partition_type = 'rrobin';
    """)
    
    result = cur.fetchone()
    if not result:
        # Chỉ chèn vào bảng chính nếu không có metadata
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
                   (userid, itemid, rating))
        openconnection.commit()
        cur.close()
        return
    
    numberofpartitions, total_rows, last_insert_idx = result
    
    # Chèn vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
               (userid, itemid, rating))
    
    # Tính toán partition kế tiếp
    next_idx = (last_insert_idx + 1) % numberofpartitions
    table_name = f"{RROBIN_TABLE_PREFIX}{next_idx}"
    
    # Chèn vào phân mảnh
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
               (userid, itemid, rating))
    
    # Cập nhật metadata
    cur.execute("""
        UPDATE partition_metadata
        SET total_rows = total_rows + 1, last_insert_idx = %s
        WHERE partition_type = 'rrobin';
    """, (next_idx,))
    
    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn dữ liệu vào bảng chính và phân mảnh theo range, dùng metadata
    """
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Lấy thông tin từ metadata
    cur.execute("""
        SELECT partition_count
        FROM partition_metadata
        WHERE partition_type = 'range';
    """)
    
    result = cur.fetchone()
    if not result:
        # Chỉ chèn vào bảng chính nếu không có metadata
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
                   (userid, itemid, rating))
        openconnection.commit()
        cur.close()
        return
    
    numberofpartitions = result[0]
    
    # Chèn vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
               (userid, itemid, rating))
    
    # Tính toán partition phù hợp
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    
    # Xử lý các trường hợp đặc biệt
    if rating % delta == 0 and index != 0:
        index -= 1
    if index >= numberofpartitions:
        index = numberofpartitions - 1
        
    table_name = f"{RANGE_TABLE_PREFIX}{index}"
    
    # Chèn vào phân mảnh
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
               (userid, itemid, rating))
    
    # Cập nhật metadata
    cur.execute("""
        UPDATE partition_metadata
        SET total_rows = total_rows + 1
        WHERE partition_type = 'range';
    """)
    
    openconnection.commit()
    cur.close()


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


def count_partitions(partition_type, openconnection):
    """
    Lấy số lượng phân mảnh từ bảng metadata
    """
    cur = openconnection.cursor()
    
    # Kiểm tra partition_type hợp lệ
    if partition_type == 'range':
        search_type = 'range'
    elif partition_type == 'rrobin_part':
        search_type = 'rrobin'
    else:
        cur.close()
        return 0
    
    cur.execute("""
        SELECT partition_count
        FROM partition_metadata
        WHERE partition_type = %s;
    """, (search_type,))
    
    result = cur.fetchone()
    count = result[0] if result else 0
    
    cur.close()
    return count
