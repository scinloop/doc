#!/usr/bin/python
# encoding: utf-8
import json
import re
import os
import mysql.connector
from mysql.connector import Error
import uuid
from datetime import datetime


# 读取JSON文件内容
def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


# 读取MD文档内容
def read_md_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()


def count_dots_with_regex(s):
    # 定义正则表达式模式，允许数字和点之间有空格
    pattern = r'^\d+(?:\s*\.\s*\d+)*'
    match = re.search(pattern, s.strip())
    if match:
        # 如果匹配成功，获取匹配到的字符串
        matched_str = match.group(0)
        # 统计匹配到的字符串中.的数量
        return matched_str.count('.')
    return 0


def count_string_in_md(file_path, target_string):
    count = 0
    first_line_number = -1
    target_stripped = target_string.replace(" ", "")
    target_stripped = target_stripped.replace("\u3000", "")
    with open(file_path, 'r', encoding='utf-8') as file:
        for line_number, line in enumerate(file, start=1):
            if line_number < 39:
                continue
            line_stripped = line.replace(" ", "")  # 去除空格
            line_stripped = line_stripped.replace('\u3000', '')
            if target_stripped in line_stripped:
                count += 1
                if first_line_number == -1:
                    first_line_number = line_number
    if count == 0:
        return -1

    return first_line_number

# 获取md文档的最后一行
def get_last_line_number(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        line_count = 0
        for line in f:
            line_count += 1
    return line_count

# 从MD文档中提取内容
def extract_content_from_md(md_file_path, md_content, json_data):
    extracted_content = []
    title_pattern1 = r'^\s*([\d.\s]+)\s*(.*?)分类编号[:：]'
    title_pattern2 = r'^\s*([\d.\s]+)\s*(.*?)(?:按照|按)\s*JT'
    title_pattern3 = r'^\s*([\d.\s]+)\s*(.*?)(?<!意)见\s*([^。\n]+)?[。]?$'
    lines = md_content.split('\n')
    num_items = len(json_data)
    previous_line = -1
    # 上一标题的startline
    for i in range(num_items):
        current_item = json_data[i]
        print(current_item['content'])
        ## 如果这一行包含 title，那么 start_line =  current_item['start_line']
        ## 否则，使用方法解析到行号
        # 删除U+3000字符（全角空格）并去除首尾空白
        content_temp = current_item['content'].replace('\u3000', '').strip()
        start_line = current_item['start_line']
        # if content_temp in lines[start_line - 1]
        if (content_temp in lines[start_line - 1]) and (start_line >= previous_line):
            start_line = current_item['start_line']
        else:
            start_line = count_string_in_md(md_file_path, content_temp)
            if start_line == -1:  # 将0改成-1，没找到
                start_line = current_item['start_line']
        end_line = start_line
        previous_line = start_line

        ## 如果包含编号啥的，就处理掉
        match1 = re.match(title_pattern1, current_item['content'])
        match2 = re.match(title_pattern2, current_item['content'])
        match3 = re.match(title_pattern3, current_item['content'])
        if match1:
            current_item['content'] = f"{match1.group(1).strip()} {match1.group(2).strip()}"
        elif match2:
            current_item['content'] = f"{match2.group(1).strip()} {match2.group(2).strip()}"
        elif match3:
            current_item['content'] = f"{match3.group(1).strip()} {match3.group(2).strip()}"

        current_item['level'] = count_dots_with_regex(current_item['content']) + 1

        # 提取标题行中除标题外的内容
        title = current_item['content'].strip()
        title_line = lines[start_line - 1]
        # 从标题行中去除标题内容
        index = title_line.find(title)
        title_line_content = title_line[index + len(title):].strip()

        if i < num_items - 1:
            # next_start_line = json_data[i + 1]['start_line']
            # 拿到下一行数据
            next_content = json_data[i + 1]['content']
            # 处理后再匹配
            match = re.match(title_pattern1, next_content)
            if match:
                next_content = f"{match.group(1).strip()} {match.group(2).strip()}"
            if i + 1 == len(json_data):
                next_start_line = get_last_line_number(md_file_path)
            else:
                next_start_line = count_string_in_md(md_file_path, next_content)
            content_between = '\n'.join(lines[start_line:next_start_line - 1])
        else:
            content_between = '\n'.join(lines[start_line:])

        # 合并标题行额外内容和中间内容
        full_content = title_line_content
        if content_between:
            if full_content:
                full_content += '\n' + content_between
            else:
                full_content = content_between

        # 去除开头和结尾的换行、空格
        full_content = full_content.strip()

        current_item['start_line'] = start_line
        current_item['end_line'] = end_line
        current_item['content'] = title
        current_item['full_content'] = full_content
        extracted_content.append(current_item)
        # 获取标题


    return extracted_content


# 连接数据库
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='172.16.2.61',
            port=3366,
            # 测试数据库
            database='cqj',
            user='root',
            password='123456',
            auth_plugin='mysql_native_password'
        )
        if connection.is_connected():
            print('Connected to MySQL database')
            return connection
    except Error as e:
        print(f'Error while connecting to MySQL: {e}')


# 获取当前最大的 inner_id
def get_max_inner_id(connection):
    try:
        cursor = connection.cursor()
        sql = "SELECT MAX(inner_id) FROM tb_document_catalog"
        cursor.execute(sql)
        result = cursor.fetchone()[0]
        return int(result) if result else 0
    except Error as e:
        print(f'Error while getting max inner_id: {e}')
        return 0


# 插入数据到tb_document表
def insert_into_tb_document(connection, document_name, file_id, file_path, creator_id):
    try:
        doc_id = str(uuid.uuid4())
        gmt_create = datetime.now()
        cursor = connection.cursor()
        sql = "INSERT INTO tb_document (id, document_name, file_id, file_path, creator_id, gmt_create) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (doc_id, document_name, file_id, file_path, creator_id, gmt_create)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Inserted into tb_document: {document_name}")
        return doc_id
    except Error as e:
        print(f'Error while inserting into tb_document: {e}')


# 插入数据到tb_document_catalog表
def insert_into_tb_document_catalog(connection, document_id, inner_id, catalog_name, parent_id, level, creator_id):
    try:
        catalog_id = str(uuid.uuid4())
        gmt_create = datetime.now()
        cursor = connection.cursor()
        sql = "INSERT INTO tb_document_catalog (id, document_id, inner_id, catalog_name, parent_id, level, creator_id, gmt_create) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        values = (catalog_id, document_id, inner_id, catalog_name, parent_id, level, creator_id, gmt_create)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Inserted  tb_document_catalog: {catalog_name}")
        return catalog_id
    except Error as e:
        print(f'Error while inserting into tb_document_catalog: {e}')


# 插入数据到tb_document_catalog_content表
def insert_into_tb_document_catalog_content(connection, catalog_id, content, page_number, creator_id):
    try:
        content_id = str(uuid.uuid4())
        gmt_create = datetime.now()
        cursor = connection.cursor()
        sql = "INSERT INTO tb_document_catalog_content (id, catalog_id, content, page_number, creator_id, gmt_create) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (content_id, catalog_id, content, page_number, creator_id, gmt_create)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Inserted into tb_document_catalog_content: {content[:20]}...")
    except Error as e:
        print(f'Error while inserting into tb_document_catalog_content: {e}')


# 插入数据到tb_document_property_content表
def insert_into_tb_document_property_content(connection, property_name, catalog_id, content, creator_id):
    try:
        property_id = str(uuid.uuid4())
        gmt_create = datetime.now()
        cursor = connection.cursor()
        sql = "INSERT INTO tb_document_property_content (id, property_name, catalog_id, content, creator_id, gmt_create) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (property_id, property_name, catalog_id, content, creator_id, gmt_create)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Inserted into tb_document_property_content: {content[:20]}...")
    except Error as e:
        print(f'Error while inserting into tb_document_property_content: {e}')


#
def calcute_parent_id(connection, document_id, inner_id, level):
    try:
        # 建立数据库连接
        if connection.is_connected():
            print('Connected to MySQL database')
            cursor = connection.cursor(dictionary=True)
            # 定义 SQL 查询语句
            query = """
                SELECT id, (%s - inner_id*1) AS chaju 
                FROM tb_document_catalog t 
                WHERE  t.document_id = %s AND t.level = (%s - 1) AND inner_id*1 < %s 
                ORDER BY chaju ASC 
                LIMIT 1;
            """
            # 执行查询
            cursor.execute(query, (inner_id, document_id, level, inner_id))
            # 获取查询结果
            result = cursor.fetchone()
            return result
    except Error as e:
        print(f'Error while executing query: {e}')


if __name__ == "__main__":
    json_file_path_arr = [
        r'./file5/交通信息基础数据元 第1部分：总则-新.json',
        r'./file5/交通信息基础数据元 第2部分：公路信息基础数据元，JTT 697.2-2014-新.json',
        r'./file5/交通信息基础数据元 第3部分：港口信息基础数据元 JTT 697.3-2013-新.json',
        r'./file2/交通信息基础数据元 第4部分：航道信息基础数据元.json',
        r'./file5/交通信息基础数据元 第5部分：船舶信息基础数据元 JTT 697.5-2013-新.json',
        r'./file2/交通信息基础数据元 第6部分：船员信息基础数据元.json',
        r'./file5/交通信息基础数据元 第7部分：道路运输信息基础数据元 JTT697.7-2022-新.json',
        r'./file5/交通运输基础数据元 第8部分：水路运输信息基础数据元 JTT697.8-2014-新.json',
        r'./file5/交通运输基础数据元 第9部分：建设项目信息基础数据元-新.json',
        r'./file5/交通信息基础数据元 第10部分：交通统计信息基础数据元，JTT 697.10-2016-新.json',
        r'./file2/交通信息基础数据元 第11部分：船舶检验信息基础数据元.json',
        r'./file5/交通运输基础数据元 第12部分：船载客货信息基础数据元 JTT697.12-2016-新.json',
        r'./file5/交通信息基础数据元 第13部分 收费公路信息基础数据元 JTT697.13-2016-新.json',
        r'./file5/交通运输基础数据元 第14部分：城市客运信息基础数据元 JTT697.14-2015-新.json',
        r'./file2/交通信息基础数据元 第15部分：航标信息基础数据元(上传系统).json',
    ]
    md_file_path_arr = [
        r'./file5/交通信息基础数据元 第1部分：总则.md',
        r'./file5/交通信息基础数据元 第2部分：公路信息基础数据元，JTT 697.2-2014.md',
        r'./file5/交通信息基础数据元 第3部分：港口信息基础数据元 JTT 697.3-2013.md',
        r'./file2/交通信息基础数据元 第4部分：航道信息基础数据元.md',
        r'./file5/交通信息基础数据元 第5部分：船舶信息基础数据元 JTT 697.5-2013.md',
        r'./file2/交通信息基础数据元 第6部分：船员信息基础数据元.md',
        r'./file5/交通信息基础数据元 第7部分：道路运输信息基础数据元 JTT697.7-2022.md',
        r'./file5/交通运输基础数据元 第8部分：水路运输信息基础数据元 JTT697.8-2014.md',
        r'./file5/交通运输基础数据元 第9部分：建设项目信息基础数据元.md',
        r'./file5/交通信息基础数据元 第10部分：交通统计信息基础数据元，JTT 697.10-2016.md',
        r'./file2/交通信息基础数据元 第11部分：船舶检验信息基础数据元.md',
        r'./file5/交通运输基础数据元 第12部分：船载客货信息基础数据元 JTT697.12-2016.md',
        r'./file5/交通信息基础数据元 第13部分 收费公路信息基础数据元 JTT697.13-2016.md',
        r'./file5/交通运输基础数据元 第14部分：城市客运信息基础数据元 JTT697.14-2015.md',
        r'./file2/交通信息基础数据元 第15部分：航标信息基础数据元(上传系统).md',
    ]

    connection = connect_to_database()

    for i in range(0, 100):
        print(f"序号 {i + 1}:")
        print(f"  json_file_path_arr 的元素: {json_file_path_arr[i]}")
        print(f"  md_file_path 的元素: {md_file_path_arr[i]}")

        json_file_path = json_file_path_arr[i]
        md_file_path = md_file_path_arr[i]

        json_data = read_json_file(json_file_path)
        md_content = read_md_file(md_file_path)

        extracted_content = extract_content_from_md(md_file_path, md_content, json_data)

        if connection:
            # 解析文档名称
            document_name = os.path.basename(md_file_path)
            document_id = insert_into_tb_document(connection, document_name, '文件存储ID', '文件存储路径', '创建人ID')
            print(f"document_id={document_id}")

            # 获取当前最大的 inner_id
            max_inner_id = get_max_inner_id(connection)

            # 初始化空
            property_lable_set = set()

            for index, item in enumerate(extracted_content):
                inner_id = max_inner_id + index + 1
                print(f'inner_id=${inner_id}')
                parent_id = '-1' if item['level'] == 1 else calcute_parent_id(connection, document_id, inner_id,
                                                                              item['level']).get('id')

                catalog_id = insert_into_tb_document_catalog(
                    connection,
                    document_id,
                    inner_id,
                    item['content'].split('\n')[0].strip(),  # 使用第一行作为目录名称
                    parent_id,  # 假设根目录的parent_id为None
                    item['level'],
                    '创建人ID'
                )
                insert_into_tb_document_catalog_content(
                    connection,
                    catalog_id,
                    item['full_content'],
                    None,  # 假设没有页码信息
                    '创建人ID'
                )

                # 分析 full_content 并插入符合格式的数据到 tb_document_property_content 表
                full_content = item['full_content'].replace('JT / T', 'JT/T')
                # 定义属性模式和对应名称
                property_patterns = [
                    (r"分类编号[：:]\s*([A-Za-z0-9]+)", "分类编号"),
                    (r"值域[: ：]\s*([^，。；\n]*)", "值域"),
                    # 匹配了按和按照，但是以按照命名
                    (r"(?:按照|按) JT/T ([^，。；\n]*)", "按照 JT/T"),
                    (r"(?:按照|按)\s+(\d+(?:\.\s*\d+)+)", "按照 JT/T"),
                    #按照本标准的4. 4. 1. 3
                    (r"按照本标准的[^\d]*(?P<number>\d+(?:\.\s*\d+)*)", "按照本标准的"),
                    (r"^见\s*([^，。；\n]*)", "见"),  # 添加^锚定行首，\s*要求"见"后有空格
                #   匹配"注：同 JT / T 697. 2—2014 的 4. 1. 1. 1"
                    (r"注[:：]同\s*JT/T\s*(\d+(?:\.\s*\d+)*—\d+\s*的\s*\d+(?:\.\s*\d+)*)", "注：同 JT/T"),
                #   匹配[来源:JT/T 697.4—-2013,5.7.1.5.2]
                    # 优化后的正则表达式，处理空格、连字符和多点分隔
                    # 优化后的正则表达式，允许编号后有额外内容
                    (r"\[来源[:：]JT/T\s*(\d+(?:\s*\.\s*\d+)*[—-]\d+,\s*\d+(?:\s*\.\s*\d+)*(?:,\s*[^\]]+)?)\]","来源：JT/T")
                ]

                # 定义默认属性（当full_content为空时插入）
                default_properties = [
                    ("空", "空"),
                    # 可以添加其他默认属性
                ]

                # 先处理默认属性（如果full_content为空）
                if not full_content.strip():
                    for prop_name, prop_value in default_properties:
                        label = f"{catalog_id}###{prop_name}"
                        if label not in property_lable_set:
                            property_lable_set.add(label)
                            insert_into_tb_document_property_content(
                                connection, prop_name, catalog_id, prop_value, '创建人ID'
                            )
                else:
                    # 原有正则匹配逻辑
                    for pattern_regex, property_name in property_patterns:
                        matches = re.findall(pattern_regex, full_content)
                        for match in matches:
                            property_value = match[0] if isinstance(match, tuple) else match
                            label = f"{catalog_id}###{property_name}"
                            if label not in property_lable_set:
                                property_lable_set.add(label)
                                insert_into_tb_document_property_content(
                                    connection, property_name, catalog_id, property_value.strip(), '创建人ID'
                                )

    connection.close()
