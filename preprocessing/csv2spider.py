"""
Converts sql+question CSV into Spider format JSON
"""

import argparse
from process_sql import get_sql
import sqlparse
import pandas as pd
import nltk
import json
nltk.download('punkt')

class SpiderQuery:
  def __init__(self, query, query_no_value, question, db_id, schema):
    self.db_id = db_id
    self.query = query

    self.question = question
    try:
      self.question_toks = nltk.word_tokenize(self.question)
      self.query_toks = nltk.word_tokenize(self.query)
      self.query_toks_no_value = nltk.word_tokenize(query_no_value)
    except:
      self.question = None
      self.query = None

    self.sql = get_sql(schema, query)

  def to_json(self):
    return vars(self)

class Schema:
  """
  Simple schema which maps table&column to a unique identifier
  """
  def __init__(self, schema, table):
    self._schema = schema
    self._table = table
    self._idMap = self._map(self._schema, self._table)

  @property
  def schema(self):
    return self._schema

  @property
  def idMap(self):
    return self._idMap

  def _map(self, schema, table):
    column_names_original = table['column_names_original']
    table_names_original = table['table_names_original']
    #print 'column_names_original: ', column_names_original
    #print 'table_names_original: ', table_names_original
    for i, (tab_id, col) in enumerate(column_names_original):
      if tab_id == -1:
        idMap = {'*': i}
      else:
        key = table_names_original[tab_id].lower()
        val = col.lower()
        idMap[key + "." + val] = i

    for i, tab in enumerate(table_names_original):
      key = tab.lower()
      idMap[key] = i

    return idMap

def get_schemas_from_json(fpath):
  with open(fpath) as f:
    data = json.load(f)
  db_names = [db['db_id'] for db in data]

  tables = {}
  schemas = {}
  for db in data:
    db_id = db['db_id']
    schema = {} #{'table': [col.lower, ..., ]} * -> __all__
    column_names_original = db['column_names_original']
    table_names_original = db['table_names_original']
    tables[db_id] = {'column_names_original': column_names_original, 'table_names_original': table_names_original}
    for i, tabn in enumerate(table_names_original):
      table = str(tabn.lower())
      cols = [str(col.lower()) for td, col in column_names_original if td == i]
      schema[table] = cols
    schemas[db_id] = schema

  return schemas, db_names, tables

def process_csv(input_file, schema, db_id, output_file):
  def visit(node, func):
    if node:
      func(node)

    if isinstance(node, sqlparse.sql.TokenList):
      for t in node.tokens:
        visit(t, func)

  def remove_value_visitor(node):
    if "Token.Literal.String" in str(node.ttype):
      node.value = "value"
    elif "Token.Literal.Number" in str(node.ttype):
      node.value = "1"

  data = pd.read_csv(input_file)

  queries = []

  for index, row in data.iterrows():
    sql_statement = sqlparse.parse(row['query'])[0]
    query = str(sql_statement)

    visit(sql_statement, remove_value_visitor)
    query_no_value = str(sql_statement)

    spider_query = SpiderQuery(query, query_no_value, row['label'], db_id, schema).to_json()
    queries.append(spider_query)

  return queries

def process(db_id, input_file, table_file, output_file):
  schemas, db_names, tables = get_schemas_from_json(table_file)
  schema = schemas[db_id]
  table = tables[db_id]
  schema = Schema(schema, table)

  queries = process_csv(args.input_file, schema, args.db_id, args.output_file)
  with open(output_file, 'w') as f:
    json.dump(queries, f, sort_keys=True, indent=2, separators=(',', ': '))

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--db_id', '-d', dest='db_id', type=str, required=True, help='Database ID to output in the json file')
  parser.add_argument('--input-file', '-i', dest='input_file', type=str, required=True, help='CSV file with two columns - "query" SQL query, "label" corresponding natural language question')
  parser.add_argument('--table-file', '-t', dest='table_file', type=str, required=True, help='JSON file with schema information in Spider format')
  parser.add_argument('--output-file', '-o', dest='output_file', type=str, required=True, help='JSON file in Spider format')
  args = parser.parse_args()

  process(args.db_id, args.input_file, args.table_file, args.output_file)

# Example usage -
# python csv2spider.py -d SS30 -i ss30_traindev.csv -t SS30/ss30_tables.json -o ss30_traindev.json
