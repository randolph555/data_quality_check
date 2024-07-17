import os
import sys
import uuid
import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# 读取 YAML 文件
def read_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# 配置数据库连接
def get_engine(config):
    password = quote_plus(config['password'])
    connection_string = f"mysql+pymysql://{config['username']}:{password}@{config['hostname']}:{config['port']}/?charset=utf8"
    return create_engine(connection_string)

def get_table_comments(engine, table_name):
    table_comment_query = text(f"""
        SELECT table_comment 
        FROM information_schema.tables 
        WHERE table_name = '{table_name.split('.')[-1]}' 
        AND table_schema = '{table_name.split('.')[0]}'
    """)
    table_comment_result = engine.execute(table_comment_query).fetchone()
    return table_comment_result['table_comment'] if table_comment_result else None

def get_column_comments(engine, table_name, column_name):
    column_comment_query = text(f"""
        SELECT column_comment 
        FROM information_schema.columns 
        WHERE table_name = '{table_name.split('.')[-1]}' 
        AND column_name = '{column_name}' 
        AND table_schema = '{table_name.split('.')[0]}'
    """)
    column_comment_result = engine.execute(column_comment_query).fetchone()
    return column_comment_result['column_comment'] if column_comment_result else None

def get_column_type(engine, table_name, column_name):
    column_type_query = text(f"""
        SELECT DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name.split('.')[-1]}' 
        AND COLUMN_NAME = '{column_name}' 
        AND TABLE_SCHEMA = '{table_name.split('.')[0]}'
    """)
    column_type_result = engine.execute(column_type_query).fetchone()
    return column_type_result['DATA_TYPE'] if column_type_result else None

# 计算数据质量报告
def calculate_data_quality(engine, table_name, columns):
    data_quality_report = []

    for column in columns:
        column_info = {
            'table_name': table_name,
            'table_comment': get_table_comments(engine, table_name),
            'column_name': column,
            'column_type': get_column_type(engine, table_name, column),
            'column_comment': get_column_comments(engine, table_name, column)
        }
        
        # 总条数
        total_query = text(f"SELECT COUNT(*) AS total FROM {table_name}")
        total_result = engine.execute(total_query).fetchone()
        column_info['total'] = total_result['total']
        
        # 计算空值率
        null_rate_query = text(f"SELECT COUNT(*) AS total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS nulls FROM {table_name}")
        null_rate_result = engine.execute(null_rate_query).fetchone()
        null_rate = null_rate_result['nulls'] / null_rate_result['total']
        column_info['null_rate'] = str(int(null_rate * 100)) + '%'
        
        # 计算重复数
        duplicate_count_query = text(f"SELECT COUNT(*) - COUNT(DISTINCT {column}) AS duplicates FROM {table_name}")
        duplicate_count_result = engine.execute(duplicate_count_query).fetchone()
        column_info['duplicate_count'] = duplicate_count_result['duplicates']
        
        # 计算最大值和最小值（如果是数值类型）
        if column_info['column_type'] in ['int', 'bigint', 'float', 'double', 'decimal']:
            min_max_query = text(f"SELECT MIN({column}) AS min_value, MAX({column}) AS max_value FROM {table_name}")
            min_max_result = engine.execute(min_max_query).fetchone()
            column_info['min_value'] = min_max_result['min_value']
            column_info['max_value'] = min_max_result['max_value']
        else:
            column_info['min_value'] = None
            column_info['max_value'] = None
        
        data_quality_report.append(column_info)
    
    return pd.DataFrame(data_quality_report)


def main(yaml_file, output_excel):
    config = read_yaml(yaml_file)
    engine = get_engine(config['source'])

    all_reports = []

    for table in config['source']['tables']:
        table_name = table['name']
        columns = table['columns']
        report = calculate_data_quality(engine, table_name, columns)
        all_reports.append(report)

    final_report = pd.concat(all_reports)
    final_report.columns = ['数据库/表', '表注释', '列名', '列类型', '列注释', '总条数', '空值率', '重复数', '最小值', '最大值']

    final_report.to_csv(output_excel, index=False)



if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_yaml_file = sys.argv[1]
    else:
        input_yaml_file = 'config.yaml' 
    output_excel = f"data_quality_report_{uuid.uuid4().hex[:8]}.csv"
    main(input_yaml_file, output_excel)
    output_path = os.path.join(os.getcwd(),output_excel)
    print(f"Data quality report saved to {output_path}")