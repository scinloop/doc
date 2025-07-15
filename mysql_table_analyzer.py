import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import os


def analyze_mysql_table(host, user, password, database, table_name, port=3306, max_unique_values=20):
    """
    连接到MySQL数据库，分析指定表的结构和内容，返回结构化分析结果
    """
    try:
        # 连接数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # 获取表结构（字段名、原生类型等）
            cursor.execute(f"DESCRIBE {table_name}")
            table_structure = cursor.fetchall()  # 每个元素为 (字段名, 原生类型, 是否可为空, ...)

            # 获取表数据
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, connection)

            # 存储分析结果的列表（每个元素是一行数据）
            analysis_results = []

            # 分析每个字段
            for column_info in table_structure:
                column_name = column_info[0]
                raw_type = column_info[1]  # 数据库原生类型（如VARCHAR(6)、DECIMAL(9,6)）
                is_nullable = column_info[2]  # 是否允许为空

                # 1. 字段名
                field_name = column_name

                # 2. 字段描述（若数据库无描述，暂填“无”）
                field_desc = "无"  # 若需要从数据库注释获取，可执行SHOW FULL COLUMNS FROM 表名

                # 3. 字段类型（转换为友好类型）
                friendly_type = convert_to_friendly_type(raw_type)

                # 4. 说明（整合缺失值、唯一值分布等信息）
                field_note = generate_field_note(df, column_name, max_unique_values)

                # 添加到结果列表
                analysis_results.append({
                    "字段名": field_name,
                    "字段描述": field_desc,
                    "字段类型": friendly_type,
                    "说明": field_note
                })

            # 转换为DataFrame表格
            result_df = pd.DataFrame(analysis_results)
            return result_df

    except Error as e:
        print(f"数据库连接错误: {e}")
        return None
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("数据库连接已关闭")


def convert_to_friendly_type(db_type):
    """将数据库原生类型转换为友好类型（如VARCHAR(6)→string(6)）"""
    db_type = db_type.lower()
    if db_type.startswith('varchar'):
        try:
            length = db_type.split('(')[1].split(')')[0]
            return f'string({length})'
        except:
            return 'string'
    elif db_type.startswith('decimal'):
        try:
            precision, scale = db_type.split('(')[1].split(')')[0].split(',')
            return f'float({precision.strip()},{scale.strip()})'
        except:
            return 'float'
    elif 'int' in db_type:
        return 'int'
    elif 'float' in db_type or 'double' in db_type:
        return 'float'
    elif 'date' in db_type or 'time' in db_type:
        return db_type
    elif 'text' in db_type:
        return 'text'
    else:
        return db_type


def generate_field_note(df, column_name, max_unique_values):
    """生成字段说明（包含缺失值、唯一值分布等）"""
    series = df[column_name]
    total = len(series)
    missing = series.isna().sum()
    note_parts = []

    # 缺失值信息
    if missing > 0:
        missing_percent = (missing / total) * 100
        note_parts.append(f"警告: 该列有 {missing} 个缺失值（占比 {missing_percent:.2f}%）")
    else:
        note_parts.append(f"缺失值数量: 0（0.00%）")

    # 唯一值分布信息
    unique_count = series.nunique()
    note_parts.append(f"唯一值数量: {unique_count}")

    # 根据数据类型处理分布详情
    if pd.api.types.is_numeric_dtype(series):
        # 数值类型：补充范围
        try:
            min_val = series.min()
            max_val = series.max()
            note_parts.append(f"数值范围: {min_val} ~ {max_val}")
        except:
            pass

        # 唯一值分布
        if unique_count <= 10:
            value_counts = series.value_counts().sort_values(ascending=False)
            dist_str = "唯一值分布 (按频率降序):\n" + "\n".join(
                [f"  值 {v}: 出现 {c} 次" for v, c in value_counts.items()])
            note_parts.append(dist_str)
        else:
            top_n = min(max_unique_values, unique_count)
            value_counts = series.value_counts().sort_values(ascending=False).head(top_n)
            dist_str = f"注意: 唯一值较多，仅显示前{top_n}个 (按频率降序):\n" + "\n".join(
                [f"  值 {v}: 出现 {c} 次" for v, c in value_counts.items()])
            note_parts.append(dist_str)

    else:
        # 字符串/对象类型
        if missing == total:
            note_parts.append("整列为空")
        else:
            # 唯一值分布
            if unique_count <= max_unique_values:
                value_counts = series.value_counts().sort_values(ascending=False)
                dist_str = "唯一值分布 (按频率降序):\n" + "\n".join(
                    [f"  '{v}': 出现 {c} 次（占比 {c / total * 100:.2f}%）" for v, c in value_counts.items()])
                note_parts.append(dist_str)
            else:
                top_n = min(max_unique_values, unique_count)
                value_counts = series.value_counts().sort_values(ascending=False).head(top_n)
                dist_str = f"注意: 唯一值较多，仅显示前{top_n}个 (按频率降序):\n" + "\n".join(
                    [f"  '{v}': 出现 {c} 次（占比 {c / total * 100:.2f}%）" for v, c in value_counts.items()])
                note_parts.append(dist_str)

    # 合并所有说明，用换行分隔
    return "\n".join(note_parts)


if __name__ == "__main__":
    # 配置数据库连接参数
    config = {
        'host': '10.60.2.187',
        'user': 'dexp',
        'password': 'lopkaTBdq62Z',
        'database': 'dexpdb',
        'table_name': 'dwd_htcl_process_round_bar_turn',
        'port': 9030,
        'max_unique_values': 20  # 最多显示的唯一值数量
    }

    tables = {
        # 'dwd_htcl_process_extrusion_line_1'
        # 'dwd_htcl_process_extrusion_line_2',
        # 'dwd_htcl_process_extrusion_line_3',
        # 'dwd_htcl_process_online_quench',
        # 'dwd_htcl_process_extrusion_stretch',
        # 'dwd_htcl_process_extrusion_anneal',
        # 'dwd_htcl_process_vertical_quench',
        # 'dwd_htcl_process_extrusion_age',
        # 'dwd_htcl_process_finished_product_saw',
        # 'dwd_htcl_process_band_saw',
        # 'dwd_htcl_process_extrusion_shape',
        # 'dwd_htcl_process_round_bar_turn'

        #热轧的
        # 'dwd_htcl_process_plate_saw',
        'dwd_htcl_process_plate_brush'


    }
    for table in tables:
        config['table_name'] = table
        # 执行分析，获取表格
        result_table = analyze_mysql_table(**config)

        if result_table is not None:
            # 自定义输出文件名（包含表名）
            output_filename = f"{config['table_name']}_字段分析报告.csv"

            # 保存为CSV（支持Excel打开，中文无乱码）
            # result_table.to_csv(output_filename, index=False, encoding='utf-8-sig')
            print(f"分析报告已保存为: {os.path.abspath(output_filename)}")

            # 可选：也支持保存为Excel
            result_table.to_excel(f"{config['table_name']}_字段分析报告.xlsx", index=False)

