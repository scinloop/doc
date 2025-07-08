import re
import pymysql  # 或根据实际数据库类型调整


# 1. 读取文件并提取ID
def extract_ids(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    # 匹配所有以XD开头后跟数字的字符串
    return re.findall(r'XD\d+', content)


# 2. 查询数据库并判断name字段
def check_names_in_db(ids):
    # 数据库连接配置（根据实际情况修改）
    conn = pymysql.connect(
        host='172.16.2.61',
        port=3366,
        # 测试数据库
        database='jzd',
        user='root',
        password='123456',
        charset='utf8mb4'
    )
    cursor = conn.cursor()

    valid_records = []

    try:
        for id_val in ids:
            sql = "SELECT flag_pos_3 FROM xd4 WHERE sale_order_sub_no = %s"
            cursor.execute(sql, (id_val,))
            result = cursor.fetchone()

            if result and result[0] is not None:
                valid_records.append(id_val)
                print(f"ID: {id_val} | Name: {result[0]}")
            # 可选：输出无效记录
            elif result is None:
                print(f"ID: {id_val} 不存在")
            # else:
            #     print(f"ID: {id_val} | Name: NULL")

    finally:
        cursor.close()
        conn.close()

    return valid_records


# 主程序
if __name__ == "__main__":
    file_path = '航空航天.txt'

    # 提取所有ID
    ids = extract_ids(file_path)
    print(f"共找到 {len(ids)} 个ID")

    # 查询数据库并过滤有效记录
    valid_ids = check_names_in_db(ids)
    print(f"共有 {len(valid_ids)} 个ID的name字段非空")