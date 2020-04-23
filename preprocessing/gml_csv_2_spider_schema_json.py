"""
Converts GML CSV into Spider artifacts - schema json file, tables json, sqlite db.
Example input: CSV generated from https://docs.google.com/spreadsheets/d/1og_gZ9oINInEO4CHpdPMIvg106qlj49fKNxVFWGL5Ww/edit#gid=1302477593
"""

import argparse
import csv
import json
import inflection
import nltk
import os
from pathlib import Path
import subprocess
import pdb

def process(args):
  output_directory = Path(args.output_directory)

  print(f"Loading GML:                     {args.gml_csv_file}")
  schema_db = load_gml(args.gml_csv_file)
  # print("Parsing GML")
  schema_dict, col_index_dict = create_schema_json(schema_db)
  all_tables = set(schema_dict.keys())

  output_json_file = output_directory / f"{args.db_id}_schema.json"
  print(f"Writing Spider schema json file: {output_json_file}")
  with open(output_json_file, "w") as f:
    json.dump(schema_dict, f, indent=2)

  tables_json = get_spider_table(schema_dict, args.db_id)
  f_keys = define_foreign_keys(schema_dict, col_index_dict)
  tables_json['foreign_keys'] = f_keys

  tables_json_filename = output_directory / f"{args.db_id}_schema_tables.json"
  print(f"Writing tables json file:        {tables_json_filename}")
  with open(tables_json_filename, 'w') as f:
    json.dump([tables_json], f, indent=2)

  sql = get_sql(tables_json, schema_dict)
  sql_file = output_directory / f"{args.db_id}.sql"
  print(f"Writing SQL file:                {sql_file}")
  with open(sql_file, 'w') as f:
    f.write(sql)

  sqlite_file = output_directory / f"{args.db_id}.sqlite"
  print(f"Creating SQLite database:        {sqlite_file}")
  retval = os.system(f"rm -f {sqlite_file} && sqlite3 {sqlite_file} < {sql_file}")

  # verify database creation
  out = subprocess.Popen(['sqlite3', sqlite_file, '.tables'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout,stderr = out.communicate()
  tables_in_db = set(stdout.decode('ascii').split())
  if all_tables != tables_in_db:
    print("ERROR: Tables written to database do no match tables in GML")
    print("   GML tables - ", ', '.join(all_tables))
    print("   DB tables  - ", ', '.join(tables_in_db))
  else:
    print("Verified DB contents")

def load_gml(gml_csv_file):
  with open(gml_csv_file) as f:
    dict_reader = csv.DictReader(f)

    required_columns = {'Table', 'Column', 'Type', 'Primary Key', 'Description', 'Joinable to'}
    all_columns = set(dict_reader.fieldnames)
    if not required_columns.issubset(all_columns):
      raise Exception(f"GML CSV file is missing required columns: {required_columns - all_columns}")

    schema_db = list(dict_reader)

  return schema_db

def underscore(s):
  return inflection.underscore(inflection.parameterize(s))

def create_schema_json(schema_db):
  n_columns = 0
  json_dict = {}
  table_dict = {}
  last_table = underscore(schema_db[0]['Table'])
  column_index_dict = {}

  for idx, item in enumerate(schema_db):
    n_columns += 1
    this_table = underscore(item['Table'])
    column = underscore(item["Column"])
    column_index_dict[this_table + '.' + column] = idx + 1
    is_primary_column = item['Primary Key'] in ['primary_key', 'Primary Key', 'yes', 'Yes']
    description = item["Description"]

    if item['Joinable to']:
      joinable_tables = item['Joinable to'].split('\n')
      final_joinable_tables = []
      for joined in joinable_tables:
        try:
          tab, col = joined.split('.')
        except ValueError:
          raise Exception(f"'Joinable to' value '{joined}' for column '{this_table + '.' + column}' must follow table.column format.")

        final_joinable_tables.append(underscore(tab.strip()) + '.' + underscore(col.strip()))
    else:
      final_joinable_tables = []

    this_dict = {column: {"type": item["Type"], "rule": column, "is_primary": is_primary_column, "joinable_to": final_joinable_tables, "description": item["Description"]}}#"where_value_options": where_values}}
    if this_table==last_table:
      table_dict = {**table_dict, **this_dict}
    else:
      if table_dict:
        new_table_dict = {last_table:table_dict}
        # print('columns in table', item['Table'], len(table_dict))
        json_dict = {**json_dict, **new_table_dict}
        table_dict = this_dict
      else:
        table_dict = this_dict

    last_table = this_table
  new_table_dict = {last_table:table_dict}
  json_dict = {**json_dict, **new_table_dict}

  if len(table_dict.keys()) < 2:
    raise Exception("GNN requires at least 2 tables. Add a dummy table with 2 dummy columns if you have a single table in your GML.")

  # Make sure join columns are valid
  all_columns = set(column_index_dict.keys())
  for table, table_info in json_dict.items():
    if len(table_info.keys()) < 2:
      raise Exception(f"GNN requires each table to have at least 2 columns. Table {table} has a single column. Add a dummy column for this table in your GML.")

    for column, column_info in table_info.items():
      if column_info['joinable_to']:
        if not set(column_info['joinable_to']).issubset(all_columns):
          raise Exception(f"Unknown column(s): {', '.join(set(column_info['joinable_to']) - all_columns)}.\n\n{table}.{column} joins with {', '.join(column_info['joinable_to'])}.\n\nSchema defines following columns: {', '.join(all_columns)}")

  return json_dict, column_index_dict

def clean_column_table_name(name):
  tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')
  inter = ' '.join(tokenizer.tokenize(name)).lower().replace('_', ' ').replace('-', ' ')
  return inter

def get_spider_table(db_info, db_id):
  new_table = {}
  new_table["column_names"] = []
  new_table["column_names_original"] = []
  new_table["column_types"] = []
  new_table["primary_keys"] = []
  new_table["foreign_keys"] = []
  new_table["table_names_original"] = []
  new_table["table_names"] = []
  new_table["db_id"] = db_id
  columns = []
  table_counter = 0
  col_counter = 1
  new_table["column_names_original"].append([-1, "*"])
  new_table["column_names"].append([-1, "*"])
  new_table["column_types"].append('text')
  for table in db_info.keys():
    for col in db_info[table]:
      new_table["column_names_original"].append([table_counter, col])
      new_table["column_names"].append([table_counter, clean_column_table_name(col)])
      type_text = db_info[table][col]['type'].lower()
      if type_text in ['integer', 'int', 'double', 'float']:
        type = "number"
      elif type_text in ['datetime', 'time']:
        type = "time"
      elif type_text in ['boolean']:
        type = "boolean"
      else:
        type = "text"
      new_table["column_types"].append(type)
      if db_info[table][col]['is_primary']:
        new_table['primary_keys'].append(col_counter)
      col_counter += 1
    table_counter += 1
    new_table["table_names_original"].append(table)
    new_table["table_names"].append(clean_column_table_name(table))
  return new_table

def define_foreign_keys(db_info, col_index_dict):
  foreign_keys_list = []
  res = []
  for table in db_info.keys():
    for col in db_info[table]:
      if db_info[table][col]["joinable_to"]:
        for join_tab_col in db_info[table][col]["joinable_to"]:
          f_list = sorted([col_index_dict[table+'.'+col], col_index_dict[join_tab_col]])
          if f_list not in foreign_keys_list:
            foreign_keys_list.append(f_list)
  return foreign_keys_list

def make_table_statement(table_name, table):
  statement = "CREATE TABLE {} (\n".format(table_name)
  for column in table.keys():
    text = table[column]['type']
    if text == "STRING":
      text = "text"
    statement += "{} {}{}".format(column, text,
                                   " PRIMARY KEY" if table[column]['is_primary'] else "")
    statement += ",\n"

  for column in table.keys():
    join_columns = table[column]['joinable_to']
    if join_columns:
      for jcol in join_columns:
        statement += "FOREIGN KEY ({}) REFERENCES {}({})".format(column, jcol.split('.')[0], jcol.split('.')[1])
        statement += ",\n"
  try:
    statement = statement[:-2]
  except:
    return None
  statement += "\n);"
  return statement

def get_sql(tables_json, schema_dict):
  list_of_tables = tables_json["table_names_original"]
  res = ""
  for table in list_of_tables:
    res += make_table_statement(table, schema_dict[table])
    res += "\n\n"
  return res

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--db_id', '-d', dest='db_id', type=str, required=True, help='Database ID')
  parser.add_argument('--gml-csv-file', '-g', dest='gml_csv_file', type=str, required=True, help='GML CSV file, e.g. created from https://docs.google.com/spreadsheets/d/1og_gZ9oINInEO4CHpdPMIvg106qlj49fKNxVFWGL5Ww/edit#gid=1302477593')
  parser.add_argument('--output-directory', '-o', dest='output_directory', type=str, required=True, help='Output directory to generate files')
  args = parser.parse_args()

  process(args)
