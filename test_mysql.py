import pymysql
import traceback

print("🔄 Starting PyMySQL connection test...")

try:
    connection = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='Iameighteeni@18',
        connect_timeout=5
    )
    print("✅ Connected using PyMySQL!")

    with connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS txt2sql")
        print("🎉 Created database 'txt2sql'")

    connection.close()

except Exception as e:
    print("❌ Failed to connect with PyMySQL:")
    traceback.print_exc()


