"""中文姓名拼音转换工具"""
import re
from typing import List, Tuple
from pypinyin import lazy_pinyin


def is_chinese_name(name: str) -> bool:
    """检测是否包含中文字符"""
    return bool(re.search(r'[\u4e00-\u9fff]', name))


def get_pinyin(name: str, style: str = 'normal') -> List[str]:
    """获取姓名的拼音表示

    Args:
        name: 中文姓名
        style: 'normal' (李樾 -> liyue), 'surname_first' (李樾 -> li-yue)

    Returns:
        拼音列表
    """
    if style == 'normal':
        return lazy_pinyin(name)
    elif style == 'surname_first':
        # 保持姓在前名在后的顺序
        from pypinyin import Style, pinyin
        return [py[0] for py in pinyin(name, style=Style.NORMAL)]
    return lazy_pinyin(name)


def split_surname_givenname(name: str) -> Tuple[str, str]:
    """分离姓和名，返回 (姓, 名)"""
    name = name.strip()
    if len(name) >= 2:
        return name[0], name[1:]
    return name, ""


def generate_search_variants(name: str) -> List[str]:
    """生成姓名搜索变体列表"""
    if not is_chinese_name(name):
        return [name]

    surname, givenname = split_surname_givenname(name)

    # 获取完整拼音
    surname_py = lazy_pinyin(surname)[0]
    givenname_py = "".join(lazy_pinyin(givenname)) if givenname else ""

    variants = []

    # 1. 名 + 姓 (Yue Li) - 国际常用格式
    if givenname_py and surname_py:
        variants.append(f"{givenname_py} {surname_py}")

    # 2. 姓 + 名 (Li Yue) - 中式顺序
    if givenname_py and surname_py:
        variants.append(f"{surname_py} {givenname_py}")

    # 3. 名字首字母 + 姓 (Y. Li)
    if givenname_py:
        variants.append(f"{givenname_py[0]}. {surname_py}")

    # 4. 保留原始中文名
    variants.append(name)

    # 去重并返回
    return list(set(variants))
