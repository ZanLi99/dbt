import psycopg2

# 定义数据库连接参数
conn_params = {
    "database": "postgres",
    "user": "postgres",
    "password": "1122",
    "host": "35.189.29.93",
    "port": "5432"
}

try:
    # 连接到数据库
    conn = psycopg2.connect(**conn_params)
    
    # 创建一个游标
    cursor = conn.cursor()
    
    # 执行一个查询
    cursor.execute("SELECT version();")
    
    # 检索查询结果
    record = cursor.fetchone()
    print("成功连接到数据库：", record)
    
except (Exception, psycopg2.Error) as error:
    print("连接到数据库时出错：", error)

finally:
    # 关闭游标和数据库连接
    if conn:
        cursor.close()
        conn.close()
        print("数据库连接已关闭")
