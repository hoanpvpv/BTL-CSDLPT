#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'C:\\Users\\hoany\\Desktop\\ml-10M100K\\ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file

import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import time  # Thêm thư viện để đo thời gian


# Hàm hỗ trợ đo thời gian
def time_function(func_name, func, *args, **kwargs):
    print(f"\n{'='*50}")
    print(f"Bắt đầu thực thi hàm {func_name}...")
    start_time = time.time()
    
    result = func(*args, **kwargs)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Hàm {func_name} đã chạy xong sau {elapsed_time:.4f} giây")
    print(f"{'='*50}\n")
    
    return result


if __name__ == '__main__':
    try:
        total_start_time = time.time()
        print("Bắt đầu kiểm thử...")
        
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            # Đo thời gian loadratings
            print("\nKiểm thử loadratings:")
            start_time = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            if result:
                print(f"loadratings function pass! Thời gian: {end_time - start_time:.4f} giây")
            else:
                print(f"loadratings function fail! Thời gian: {end_time - start_time:.4f} giây")

            # Đo thời gian rangepartition
            print("\nKiểm thử rangepartition:")
            start_time = time.time()
            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            if result:
                print(f"rangepartition function pass! Thời gian: {end_time - start_time:.4f} giây")
            else:
                print(f"rangepartition function fail! Thời gian: {end_time - start_time:.4f} giây")

            # Đo thời gian rangeinsert
            print("\nKiểm thử rangeinsert:")
            start_time = time.time()
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            end_time = time.time()
            if result:
                print(f"rangeinsert function pass! Thời gian: {end_time - start_time:.4f} giây")
            else:
                print(f"rangeinsert function fail! Thời gian: {end_time - start_time:.4f} giây")

            # Xóa và tạo lại bảng để kiểm thử round robin
            testHelper.deleteAllPublicTables(conn)
            
            # Đo thời gian loadratings lần thứ 2
            print("\nNạp lại dữ liệu cho kiểm thử round robin:")
            start_time = time.time()
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
            end_time = time.time()
            print(f"Nạp lại dữ liệu hoàn tất! Thời gian: {end_time - start_time:.4f} giây")

            # Đo thời gian roundrobinpartition
            print("\nKiểm thử roundrobinpartition:")
            start_time = time.time()
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            if result:
                print(f"roundrobinpartition function pass! Thời gian: {end_time - start_time:.4f} giây")
            else:
                print(f"roundrobinpartition function fail! Thời gian: {end_time - start_time:.4f} giây")

            # Đo thời gian roundrobininsert
            print("\nKiểm thử roundrobininsert:")
            start_time = time.time()
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
            end_time = time.time()
            if result:
                print(f"roundrobininsert function pass! Thời gian: {end_time - start_time:.4f} giây")
            else:
                print(f"roundrobininsert function fail! Thời gian: {end_time - start_time:.4f} giây")

            # Tính tổng thời gian kiểm thử
            total_end_time = time.time()
            print(f"\nTổng thời gian kiểm thử: {total_end_time - total_start_time:.4f} giây")

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        # Vẫn hiển thị thời gian ngay cả khi có lỗi
        total_end_time = time.time()
        print(f"\nKiểm thử thất bại sau {total_end_time - total_start_time:.4f} giây")
        traceback.print_exc()
