import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
import re
from scipy import stats
import seaborn as sns
from datetime import datetime


class MySQLTableAnalyzer:
    """MySQL表结构和数据内容分析工具"""

    def __init__(self, host, user, password, database, table_name, port=3306):
        """初始化分析器"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name
        self.port = port
        self.connection = None
        self.data = None
        self.structure = None
        self.report = []

    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            if self.connection.is_connected():
                print(f"成功连接到数据库: {self.database}")
                return True
        except Error as e:
            print(f"数据库连接错误: {e}")
            return False

    def disconnect(self):
        """断开数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("数据库连接已关闭")

    def fetch_table_structure(self):
        """获取表结构信息"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False

        cursor = self.connection.cursor()
        cursor.execute(f"DESCRIBE {self.table_name}")
        self.structure = cursor.fetchall()
        cursor.close()

        print(f"已获取表 '{self.table_name}' 的结构信息")
        return True

    def fetch_table_data(self, sample_size=None):
        """
        获取表数据，可以选择获取全量数据或抽样数据

        参数:
        sample_size (int): 抽样大小，如果为None则获取全量数据
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False

        if sample_size:
            # 使用SQL LIMIT进行抽样
            query = f"SELECT * FROM {self.table_name} LIMIT {sample_size}"
            print(f"正在从表 '{self.table_name}' 中抽样 {sample_size} 条记录...")
        else:
            # 获取全量数据
            query = f"SELECT * FROM {self.table_name}"
            print(f"正在从表 '{self.table_name}' 中获取全量数据...")

        self.data = pd.read_sql(query, self.connection)
        print(f"数据获取完成，共 {len(self.data)} 条记录")
        return True

    def analyze_column_data_types(self):
        """分析列的实际数据类型"""
        if self.data is None or self.data.empty:
            print("没有数据可供分析")
            return

        self.report.append("=== 列数据类型分析 ===")

        for col in self.data.columns:
            inferred_type = self._infer_data_type(self.data[col])
            sql_type = next((info[1] for info in self.structure if info[0] == col), "未知")

            self.report.append(f"{col}:")
            self.report.append(f"  SQL定义类型: {sql_type}")
            self.report.append(f"  实际推断类型: {inferred_type}")
            self.report.append("")

    def _infer_data_type(self, series):
        """推断Series的数据类型"""
        # 处理缺失值
        non_null = series.dropna()
        if non_null.empty:
            return "全部缺失值"

        # 检查是否为数值类型
        if pd.api.types.is_numeric_dtype(non_null):
            # 检查是否为整数
            if np.issubdtype(non_null.dtype, np.integer):
                return "整数"
            else:
                return "浮点数"

        # 检查是否为日期时间类型
        try:
            pd.to_datetime(non_null, errors='raise')
            return "日期时间"
        except (TypeError, ValueError):
            pass

        # 检查是否为布尔类型
        unique_values = non_null.unique()
        if set(unique_values).issubset({True, False, 1, 0}):
            return "布尔值"

        # 检查是否为分类类型
        if len(unique_values) < 0.1 * len(non_null) and len(unique_values) < 50:
            return f"分类 (唯一值数: {len(unique_values)})"

        # 检查是否为身份证号码
        if all(non_null.astype(str).str.match(r'^\d{17}[\dXx]$')):
            return "身份证号码"

        # 检查是否为手机号码
        if all(non_null.astype(str).str.match(r'^1[3-9]\d{9}$')):
            return "手机号码"

        # 检查是否为邮箱地址
        email_pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        if all(non_null.astype(str).str.match(email_pattern)):
            return "邮箱地址"

        # 检查是否为URL
        url_pattern = r'^https?://(?:www\.)?[^\s/$.?#].[^\s]*$'
        if all(non_null.astype(str).str.match(url_pattern)):
            return "URL"

        # 检查是否为JSON字符串
        try:
            if all(non_null.astype(str).apply(lambda x: self._is_json(x))):
                return "JSON字符串"
        except:
            pass

        # 默认返回字符串
        return "字符串"

    def _is_json(self, s):
        """检查字符串是否为JSON格式"""
        try:
            import json
            json.loads(s)
            return True
        except (json.JSONDecodeError, TypeError):
            return False

    def analyze_column_distribution(self, max_unique_values=20):
        """分析列的数据分布"""
        if self.data is None or self.data.empty:
            print("没有数据可供分析")
            return

        self.report.append("=== 列数据分布分析 ===")

        for col in self.data.columns:
            self.report.append(f"列名: {col}")

            # 基本统计信息
            missing_count = self.data[col].isna().sum()
            unique_count = self.data[col].nunique()
            total_count = len(self.data)

            self.report.append(f"  总记录数: {total_count}")
            self.report.append(f"  缺失值数: {missing_count} ({missing_count / total_count * 100:.2f}%)")
            self.report.append(f"  唯一值数: {unique_count} ({unique_count / total_count * 100:.2f}%)")

            # 根据数据类型进行不同的分析
            dtype = self.data[col].dtype

            if pd.api.types.is_numeric_dtype(dtype):
                # 数值类型分析
                self._analyze_numeric_column(col)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                # 日期时间类型分析
                self._analyze_datetime_column(col)
            else:
                # 字符串或其他类型分析
                self._analyze_categorical_column(col, max_unique_values)

            self.report.append("")  # 添加空行分隔不同列的报告

    def _analyze_numeric_column(self, col):
        """分析数值类型列"""
        series = self.data[col].dropna()

        # 基本统计量
        min_val = series.min()
        max_val = series.max()
        mean_val = series.mean()
        median_val = series.median()
        std_dev = series.std()

        self.report.append(f"  数值范围: {min_val} ~ {max_val}")
        self.report.append(f"  平均值: {mean_val:.4f}")
        self.report.append(f"  中位数: {median_val}")
        self.report.append(f"  标准差: {std_dev:.4f}")

        # 分位数
        q1, q3 = series.quantile([0.25, 0.75])
        iqr = q3 - q1
        self.report.append(f"  四分位数: Q1={q1}, Q3={q3}, IQR={iqr}")

        # 异常值检测
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        self.report.append(f"  异常值数量: {len(outliers)} ({len(outliers) / len(series) * 100:.2f}%)")

        # 分布分析
        unique_values = series.nunique()
        if unique_values <= 10:
            # 唯一值较少时显示全量分布
            self.report.append("  唯一值分布 (按频率降序):")
            for value, count in series.value_counts().items():
                self.report.append(f"    值 {value}: 出现 {count} 次 ({count / len(series) * 100:.2f}%)")
        else:
            # 唯一值较多时显示频率最高的几个
            self.report.append("  最频繁出现的值 (按频率降序):")
            for value, count in series.value_counts().head(10).items():
                self.report.append(f"    值 {value}: 出现 {count} 次 ({count / len(series) * 100:.2f}%)")

        # 偏度和峰度
        skewness = series.skew()
        kurtosis = series.kurt()
        self.report.append(f"  偏度: {skewness:.4f}")
        self.report.append(f"  峰度: {kurtosis:.4f}")

    def _analyze_datetime_column(self, col):
        """分析日期时间类型列"""
        series = self.data[col].dropna()

        # 基本统计量
        min_date = series.min()
        max_date = series.max()
        date_range = max_date - min_date

        self.report.append(f"  日期范围: {min_date} ~ {max_date}")
        self.report.append(f"  时间跨度: {date_range.days} 天")

        # 按年/月/日/周几统计
        yearly_counts = series.dt.year.value_counts().sort_index()
        monthly_counts = series.dt.month.value_counts().sort_index()
        daily_counts = series.dt.day.value_counts().sort_index()
        weekday_counts = series.dt.weekday.value_counts().sort_index()

        if not yearly_counts.empty:
            self.report.append("  按年份分布:")
            for year, count in yearly_counts.items():
                self.report.append(f"    {year}: {count} 条记录")

        if not monthly_counts.empty:
            self.report.append("  按月份分布:")
            for month, count in monthly_counts.items():
                month_name = pd.Timestamp(f'2000-{month}-01').month_name()
                self.report.append(f"    {month_name}: {count} 条记录")

        if not daily_counts.empty:
            self.report.append("  按日期分布:")
            for day, count in daily_counts.items():
                self.report.append(f"    每月{day}日: {count} 条记录")

        if not weekday_counts.empty:
            self.report.append("  按星期分布:")
            weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
            for weekday, count in weekday_counts.items():
                self.report.append(f"    {weekday_names[weekday]}: {count} 条记录")

    def _analyze_categorical_column(self, col, max_unique_values=20):
        """分析分类类型列"""
        series = self.data[col].dropna()
        unique_values = series.nunique()

        if unique_values == 0:
            self.report.append("  所有值均为缺失值")
            return

        if unique_values == 1:
            self.report.append("  该列所有值均相同")
            self.report.append(f"  唯一值: {series.iloc[0]}")
            return

        # 计算频率分布
        value_counts = series.value_counts(normalize=True) * 100

        if unique_values <= max_unique_values:
            # 唯一值较少时显示全量分布
            self.report.append("  唯一值分布 (按频率降序):")
            for value, pct in value_counts.items():
                count = int(pct * len(series) / 100)
                self.report.append(f"    '{value}': 出现 {count} 次 ({pct:.2f}%)")
        else:
            # 唯一值较多时显示频率最高的几个
            self.report.append(f"  最频繁出现的 {max_unique_values} 个值 (按频率降序):")
            for value, pct in value_counts.head(max_unique_values).items():
                count = int(pct * len(series) / 100)
                self.report.append(f"    '{value}': 出现 {count} 次 ({pct:.2f}%)")
            self.report.append(
                f"  其他 {unique_values - max_unique_values} 个唯一值占 {value_counts.iloc[max_unique_values:].sum():.2f}%")

    def analyze_data_quality(self):
        """分析数据质量"""
        if self.data is None or self.data.empty:
            print("没有数据可供分析")
            return

        self.report.append("=== 数据质量分析 ===")

        # 整体缺失情况
        total_cells = self.data.size
        missing_cells = self.data.isna().sum().sum()
        missing_percentage = missing_cells / total_cells * 100

        self.report.append(f"总单元格数: {total_cells}")
        self.report.append(f"缺失单元格数: {missing_cells} ({missing_percentage:.2f}%)")

        # 各列缺失情况
        self.report.append("\n各列缺失情况:")
        missing_stats = self.data.isna().mean().sort_values(ascending=False) * 100
        for col, pct in missing_stats.items():
            if pct > 0:
                self.report.append(f"  {col}: {pct:.2f}% 缺失")

        # 完全重复的行
        duplicate_rows = self.data.duplicated().sum()
        self.report.append(f"\n完全重复的行数: {duplicate_rows} ({duplicate_rows / len(self.data) * 100:.2f}%)")

        # 主键候选分析
        self.report.append("\n主键候选分析:")
        for col in self.data.columns:
            unique_ratio = self.data[col].nunique() / len(self.data)
            if unique_ratio >= 0.99:
                self.report.append(f"  {col}: 唯一值比例 {unique_ratio * 100:.2f}%，可能是主键")
            elif unique_ratio >= 0.9:
                self.report.append(f"  {col}: 唯一值比例 {unique_ratio * 100:.2f}%，接近主键")
            else:
                self.report.append(f"  {col}: 唯一值比例 {unique_ratio * 100:.2f}%")

        # 异常值检测汇总
        self.report.append("\n数值列异常值检测:")
        for col in self.data.select_dtypes(include=[np.number]).columns:
            series = self.data[col].dropna()
            if len(series) < 10:  # 数据点太少不进行分析
                continue

            q1, q3 = series.quantile([0.25, 0.75])
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = series[(series < lower_bound) | (series > upper_bound)]

            if len(outliers) > 0:
                self.report.append(f"  {col}: {len(outliers)} 个异常值 ({len(outliers) / len(series) * 100:.2f}%)")

    def analyze_correlations(self):
        """分析列之间的相关性"""
        if self.data is None or self.data.empty:
            print("没有数据可供分析")
            return

        self.report.append("=== 相关性分析 ===")

        # 数值列之间的相关性
        numeric_cols = self.data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) < 2:
            self.report.append("数值列不足，无法进行相关性分析")
            return

        # 计算相关系数矩阵
        corr_matrix = self.data[numeric_cols].corr()

        self.report.append("数值列皮尔逊相关系数:")
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                col1 = numeric_cols[i]
                col2 = numeric_cols[j]
                corr = corr_matrix.loc[col1, col2]
                self.report.append(f"  {col1} 和 {col2}: {corr:.4f}")

        # 热力图生成（如果有matplotlib）
        try:
            plt.figure(figsize=(10, 8))
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f')
            plt.title('数值列相关性热力图')
            plt.tight_layout()
            plt.savefig(f"{self.table_name}_correlation_heatmap.png")
            self.report.append("\n已生成相关性热力图: correlation_heatmap.png")
        except Exception as e:
            self.report.append(f"\n生成相关性热力图失败: {e}")

    def generate_report(self, output_file=None):
        """生成分析报告"""
        if not self.report:
            print("没有分析结果可供生成报告")
            return

        # 添加报告头部
        header = [
            f"MySQL表 '{self.table_name}' 分析报告",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"数据记录数: {len(self.data) if self.data is not None else 0}",
            "-" * 50
        ]

        full_report = "\n".join(header + self.report)

        # 打印报告
        print(full_report)

        # 保存到文件
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(full_report)
            print(f"\n报告已保存到: {output_file}")

        return full_report


if __name__ == "__main__":
    # 配置数据库连接参数
    config = {
        'host': '10.60.2.187',
        'user': 'dexp',
        'password': 'lopkaTBdq62Z',
        'database': 'dexpdb',
        'table_name': 'dwd_htcl_process_extrusion_line_1',
        'port': 9030  # 添加了端口参数
    }

    # 创建分析器实例
    analyzer = MySQLTableAnalyzer(**config)

    # 执行分析
    if analyzer.connect():
        if analyzer.fetch_table_structure() and analyzer.fetch_table_data():
            # 分析列数据类型
            analyzer.analyze_column_data_types()

            # 分析列分布
            analyzer.analyze_column_distribution()

            # 分析数据质量
            analyzer.analyze_data_quality()

            # 分析相关性
            analyzer.analyze_correlations()

            # 生成报告
            analyzer.generate_report(f"{config['table_name']}_analysis_report.txt")

        analyzer.disconnect()