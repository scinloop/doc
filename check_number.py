import re
import logging
from collections import defaultdict
from typing import List, Tuple, Dict, Set, Optional, Any, Callable
import pymysql
from pymysql.cursors import Cursor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NumberingError:
    """表示编号验证过程中的错误"""

    def __init__(self, code: str, message: str, number: Optional[Tuple[int, ...]] = None, doc_id: Optional[int] = None):
        self.code = code
        self.message = message
        self.number = number
        self.doc_id = doc_id

    def __str__(self):
        doc_info = f" (文档ID: {self.doc_id})" if self.doc_id else ""
        return f"[{self.code}]{doc_info} {self.message}"


def parse_numbering(lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[NumberingError]]:
    """解析每行数据中的编号结构，返回包含编号信息的列表和错误列表。"""
    numbering_list: List[Dict[str, Any]] = []
    errors: List[NumberingError] = []

    for line in lines:
        doc_id = line['document_id']
        content = line['catalog_name'].strip()
        match = re.match(r'^(\d+(?:[.．]\s*\d+)*)', content)
        if not match:
            errors.append(NumberingError("PARSE001", f"无法在内容中找到编号: '{content}'", None, doc_id))
            continue

        number_str = match.group(1).strip()
        number_str = number_str.replace('．', '.').replace('\u3000', ' ')
        number_str = re.sub(r'\.\s*', '.', number_str)

        parts = [p.strip() for p in number_str.split('.') if p.strip()]
        try:
            levels = [int(p) for p in parts]
        except ValueError:
            errors.append(NumberingError("PARSE002",
                                         f"格式错误：无法解析编号 '{number_str}'（包含非数字字符）", tuple(parts), doc_id))
            continue

        if not levels:
            errors.append(NumberingError("PARSE003", f"解析后的编号为空: '{number_str}'", None, doc_id))
            continue

        numbering_list.append({
            'document_id': doc_id,
            'catalog_name': line['catalog_name'],
            'number_tuple': tuple(levels),
            'number_str': '.'.join(map(str, levels))
        })

    return numbering_list, errors


def validate_parent_existence(numbering_list: List[Dict[str, Any]]) -> List[NumberingError]:
    """验证每个子编号的父编号是否存在"""
    errors: List[NumberingError] = []
    doc_numbers: Dict[int, Set[Tuple[int, ...]]] = defaultdict(set)
    for item in numbering_list:
        doc_numbers[item['document_id']].add(item['number_tuple'])

    for item in numbering_list:
        number = item['number_tuple']
        doc_id = item['document_id']
        if len(number) > 1:
            parent = number[:-1]
            if parent not in doc_numbers[doc_id]:
                errors.append(NumberingError("VALID001",
                                             f"结构错误：编号 {number} 的父编号 {parent} 不存在", number, doc_id))
    return errors


def validate_children(numbering_list: List[Dict[str, Any]]) -> List[NumberingError]:
    """验证子编号的连续性、唯一性和合法性"""
    errors: List[NumberingError] = []
    doc_parent_children: Dict[int, Dict[Tuple[int, ...], List[int]]] = defaultdict(lambda: defaultdict(list))

    for item in numbering_list:
        doc_id = item['document_id']
        number = item['number_tuple']
        parent = number[:-1] if len(number) > 1 else tuple()
        doc_parent_children[doc_id][parent].append(number[-1])

    for doc_id, parent_children in doc_parent_children.items():
        for parent, children in parent_children.items():
            # 新增：忽略父编号只有一个数字的情况
            if len(parent) == 1:
                continue

            if not children:
                errors.append(NumberingError("VALID002",
                                             f"结构错误：父编号 {parent} 下没有子编号", parent, doc_id))
                continue

            unique_children = list(set(children))
            if len(unique_children) < len(children):
                duplicates = [x for x in unique_children if children.count(x) > 1]
                errors.append(NumberingError("VALID003",
                                             f"结构错误：父编号 {parent} 下存在重复编号：{duplicates}", parent, doc_id))

            invalid = [c for c in unique_children if c <= 0]
            if invalid:
                errors.append(NumberingError("VALID004",
                                             f"结构错误：父编号 {parent} 下存在非法编号（必须大于0）：{invalid}", parent,
                                             doc_id))
                continue

            children_sorted = sorted(unique_children)
            expected = list(range(1, children_sorted[-1] + 1))
            if children_sorted != expected:
                missing = sorted(set(expected) - set(children_sorted))
                errors.append(NumberingError("VALID005",
                                             f"结构错误：父编号 {parent} 下编号不连续，缺少：{missing}", parent, doc_id))
    return errors


def validate_numbering(numbering_list: List[Dict[str, Any]]) -> List[NumberingError]:
    """验证编号结构，返回错误列表"""
    errors = []
    errors.extend(validate_parent_existence(numbering_list))
    errors.extend(validate_children(numbering_list))
    return errors


def get_db_config() -> Dict[str, Any]:
    """获取数据库配置"""
    return {
        'host': '172.16.7.163',
        'port': 13306,
        'database': 'cxj',
        'user': 'adp',
        'password': 'R7#pL9@qS6!wN2',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }


def fetch_catalog_data() -> List[Dict[str, Any]]:
    """从数据库获取目录数据，包含document_id和document_name"""
    db_config = get_db_config()
    try:
        with pymysql.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT 
                    dc.document_id, 
                    d.document_name,
                    dc.catalog_name,
                    dc.inner_id
                FROM 
                    tb_document_catalog dc
                JOIN 
                    tb_document d ON dc.document_id = d.id
                ORDER BY 
                    dc.document_id, dc.inner_id
                """
                cursor.execute(query)
                return cursor.fetchall()
    except pymysql.MySQLError as e:
        logger.error(f"数据库操作失败: {e}")
        raise


def extract_part_number(doc_name: str) -> int:
    """从文档名称中提取'第x部分'的数字x，无法提取时返回无穷大"""
    match = re.search(r'第(\d+)部分', doc_name)
    if match:
        return int(match.group(1))
    return float('inf')  # 无法提取时排在最后


def main():
    """主函数"""
    try:
        logger.info("开始获取文档目录数据...")
        catalogs = fetch_catalog_data()
        logger.info(f"成功获取 {len(catalogs)} 条目录数据")

        if not catalogs:
            logger.info("没有找到目录数据，程序终止")
            return

        logger.info("开始解析编号...")
        numbering_list, parse_errors = parse_numbering(catalogs)
        for error in parse_errors:
            logger.warning(error)

        logger.info(f"解析完成，有效编号: {len(numbering_list)}, 解析错误: {len(parse_errors)}")

        if not numbering_list:
            logger.error("没有找到有效的编号，程序终止")
            return

        # 按文档ID分组
        doc_numbering: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for item in numbering_list:
            doc_numbering[item['document_id']].append(item)

        # 准备文档信息并按"第x部分"排序
        doc_infos = []
        for doc_id in doc_numbering:
            doc_info = next((c for c in catalogs if c['document_id'] == doc_id), None)
            if doc_info:
                part_num = extract_part_number(doc_info['document_name'])
                doc_infos.append({
                    'id': doc_id,
                    'name': doc_info['document_name'],
                    'part_num': part_num,
                    'numbering': doc_numbering[doc_id]
                })

        # 按part_num排序，无法提取的排在最后
        doc_infos.sort(key=lambda x: x['part_num'])

        total_validation_errors: List[NumberingError] = []

        logger.info("开始验证编号结构（按'第x部分'排序）...")

        # 按排序后的顺序处理每个文档
        for doc_info in doc_infos:
            doc_id = doc_info['id']
            doc_name = doc_info['name']
            part_num = doc_info['part_num']

            # 显示文档标题（包含部分编号）
            part_info = f"第{part_num}部分 " if part_num != float('inf') else ""
            logger.info(f"\n=== 开始处理{part_info}{doc_name} (ID: {doc_id}) ===")

            # 验证当前文档的编号
            doc_validation_errors = validate_numbering(doc_info['numbering'])
            total_validation_errors.extend(doc_validation_errors)

            # 输出当前文档的错误
            if doc_validation_errors:
                logger.warning(f"  ⚠️ 发现 {len(doc_validation_errors)} 个编号错误:")
                for error in doc_validation_errors:
                    logger.warning(f"  - {error}")
            else:
                logger.info("  ✅ 该文档编号结构正确")

        # 输出全局统计
        logger.info(f"\n验证完成，共发现 {len(total_validation_errors)} 个验证错误")

        if parse_errors or total_validation_errors:
            logger.warning(f"处理完成，共发现 {len(parse_errors) + len(total_validation_errors)} 个错误")
        else:
            logger.info("所有编号验证通过，结构正确")

    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)


if __name__ == '__main__':
    main()