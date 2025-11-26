import json
from xml.dom import minidom
import xml.etree.ElementTree as ET

try:
    from defusedxml.ElementTree import parse, tostring, fromstring
except ImportError:
    from xml.etree.ElementTree import parse, tostring, fromstring

from file_manager import write_file, read_file


def write_json(path: str, json_str: str, user_id: int, user_dir: str, ignore_null: bool = False, write_indented: bool = True) -> None:
    try:
        data = json.loads(json_str)
        if ignore_null:
            data = {k: v for k, v in data.items() if v is not None}
    except json.JSONDecodeError as e:
        raise ValueError(f"Неверный JSON: {e}")

    indent = 2 if write_indented else None
    content = json.dumps(data, indent=indent, ensure_ascii=False).encode("utf-8")
    write_file(path, content, user_id, user_dir)


def read_json(path: str, user_id: int, user_dir: str) -> str:
    content_bytes = read_file(path, user_id, user_dir)
    content = content_bytes.decode("utf-8")
    data = json.loads(content)
    return json.dumps(data, indent=2, ensure_ascii=False)


def write_xml(path: str, xml_str: str, user_id: int, user_dir: str) -> None:
    try:
        # защита от XXE с помощью defusedxml
        root = fromstring(xml_str.encode("utf-8"))
    except Exception as e:
        raise ValueError(f"Неверный или небезопасный XML: {e}")

    rough = tostring(root, encoding="unicode")
    reparsed = minidom.parseString(rough)
    pretty = reparsed.toprettyxml(indent="  ").encode("utf-8")
    write_file(path, pretty, user_id, user_dir)


def read_xml(path: str, user_id: int, user_dir: str) -> str:
    content_bytes = read_file(path, user_id, user_dir)
    content = content_bytes.decode("utf-8")
    root = fromstring(content)
    rough = tostring(root, encoding="unicode")
    reparsed = minidom.parseString(rough)
    return reparsed.toprettyxml(indent="  ")


def edit_xml_add_element(path: str, xpath: str, new_element_name: str, new_value: str, user_id: int, user_dir: str) -> None:
    content_bytes = read_file(path, user_id, user_dir)
    content = content_bytes.decode("utf-8")

    # безопасный парсинг с защитой от XXE
    root = fromstring(content)

    # обработка XPath
    if xpath.strip() in (".", "", "/"):
        parent = root
    else:
        parent = root.find(xpath)
        if parent is None:
            parent = root.find(f".//{xpath}")
        if parent is None:
            raise ValueError(f"Не найден элемент по XPath: {xpath}")

    # новый элемент
    new_elem = ET.Element(new_element_name)
    new_elem.text = new_value
    parent.append(new_elem)

    rough = tostring(root, encoding="unicode")
    reparsed = minidom.parseString(rough)
    pretty_xml = reparsed.toprettyxml(indent="  ").encode("utf-8")

    write_file(path, pretty_xml, user_id, user_dir)
    print("Элемент добавлен в XML!")