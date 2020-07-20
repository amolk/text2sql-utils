import argparse
import yaml
import json
import os

from pathlib import Path


class GmlParser:
    def __init__(self, file_path):
        # TODO: Catch exception here
        with open(file_path, 'r') as f:
            self._gml_data = yaml.load(f, Loader=yaml.BaseLoader)

        # Validate data
        tables = self._gml_data.get('schema').get('tables')

        if len(tables) < 2:
            raise Exception(
                "GNN requires at least 2 tables. "
                "Add a dummy table with 2 dummy columns if you have a single table in your GML.")

    def _parse_column_names(self):
        original_columns = [(-1, '*')]
        columns = [(-1, '*')]

        tables = self._gml_data.get('schema').get('tables')

        for tbl_idx, tbl in enumerate(tables):
            for col in tbl.get('columns'):
                original_columns.append(
                    (tbl_idx, col.get('name'))
                )

                columns.append(
                    (tbl_idx, col.get('name').lower().replace("_", ' '))
                )

        return original_columns, columns

    def _parse_column_types(self):
        column_types = ['text']

        tables = self._gml_data.get('schema').get('tables')

        for tbl in tables:
            for col in tbl.get('columns'):
                col_type = col.get('type').lower()

                if 'char' in col_type or col_type == '' or 'text' in col_type or 'var' in col_type or 'string' in col_type:
                    column_types.append('text')
                elif 'int' in col_type or 'numeric' in col_type or 'decimal' in col_type or 'number' in col_type \
                        or 'id' in col_type or 'real' in col_type or 'double' in col_type or 'float' in col_type:
                    column_types.append('number')
                elif 'date' in col_type or 'time' in col_type or 'year' in col_type:
                    column_types.append('time')
                elif 'boolean' in col_type:
                    column_types.append('boolean')
                else:
                    column_types.append('others')

        return column_types

    def _parse_primary_keys(self):
        keys = []

        tables = self._gml_data.get('schema').get('tables')

        col_idx = 1
        for tbl in tables:
            for col in tbl.get('columns'):
                constraint = col.get('key_constraint')
                if constraint == 'primary':
                    keys.append(col_idx)

                col_idx += 1

        return keys

    def _parse_foreign_keys(self, table_names=[], column_names=[]):
        foreign_keys = []

        joins = self._gml_data.get('query_generator').get('joins')

        for j in joins:
            ref_cid, cid = None, None

            table_split = j["join"].split(':')

            tbl_name, col_name = table_split[0].split('.')
            ref_tbl_name, ref_col_name = table_split[1].split('.')

            table_id = table_names.index(tbl_name)
            ref_table_id = table_names.index(ref_tbl_name)

            for i, (tab_id, col_org) in enumerate(column_names):
                if tab_id == ref_table_id and ref_col_name == col_org:
                    ref_cid = i
                elif table_id == tab_id and col_name == col_org:
                    cid = i
            if ref_cid and cid:
                foreign_keys.append([cid, ref_cid])

        return foreign_keys

    def _get_foreign_columns(self, table_name, column_name):
        ref_columns = []

        joins = self._gml_data.get('query_generator').get('joins')

        for j in joins:
            table_split = j["join"].split(':')

            tbl_name, col_name = table_split[0].split('.')
            ref_tbl_name, ref_col_name = table_split[1].split('.')

            if tbl_name == table_name and col_name == column_name:
                ref_columns.append(table_split[1])

        return ref_columns

    def _parse_table_names(self):
        tables = self._gml_data.get('schema').get('tables')

        original_names = []
        names = []

        for tbl in tables:
            tbl_name = tbl.get('name')

            original_names.append(tbl_name)
            names.append(tbl_name.lower().replace("_", ' '))

        return original_names, names

    def get_gml_schema_id(self):
        return self._gml_data["schema"]["id"]

    def convert_gml_to_schema(self):
        schema = dict()

        tables = self._gml_data.get('schema').get('tables')

        for tbl in tables:
            tbl_name = tbl.get('name')

            schema[tbl_name] = dict()

            for col in tbl.get('columns'):
                col_name = col.get('name')

                schema[tbl_name][col_name] = {
                    "type": col.get("type"),
                    "rule": col_name,
                    "is_primary": True if col.get("key_constraint") == "primary" else False,
                    "joinable_to": self._get_foreign_columns(table_name=tbl_name, column_name=col_name),
                    "description": col.get("description")
                }

        return schema

    def convert_gml_to_gnn_schema_reader(self):
        schema = dict()

        schema["db_id"] = self.get_gml_schema_id()
        schema["column_types"] = self._parse_column_types()
        schema["primary_keys"] = self._parse_primary_keys()
        schema["column_names_original"], schema["column_names"] = self._parse_column_names()
        schema["table_names_original"], schema["table_names"] = self._parse_table_names()

        schema["foreign_keys"] = self._parse_foreign_keys(
            table_names=schema.get("table_names_original"),
            column_names=schema.get("column_names_original")
        )

        return schema


def process(args):
    output_directory = Path(args.output_directory)

    print(f"Loading GML:                     {args.gml_file}")

    gml_parser = GmlParser(args.gml_file)
    table_json = gml_parser.convert_gml_to_gnn_schema_reader()

    tables_json_filename = output_directory / f"{gml_parser.get_gml_schema_id()}_schema_tables.json"

    print(f"Writing tables json file:        {tables_json_filename}")
    with open(tables_json_filename, 'w') as f:
        json.dump([table_json], f, indent=2)

    # Create sql file
    sql = get_sql(table_json, gml_parser.convert_gml_to_schema())
    sql_file = output_directory / f"{gml_parser.get_gml_schema_id()}.sql"
    print(f"Writing SQL file:                {sql_file}")
    with open(sql_file, 'w') as f:
        f.write(sql)

    # Create sqlite db
    sqlite_file = output_directory / f"{gml_parser.get_gml_schema_id()}.sqlite"
    print(f"Creating SQLite database:        {sqlite_file}")
    os.system(f"rm -f {sqlite_file} && sqlite3 {sqlite_file} < {sql_file}")


def get_sql(tables_json, schema_dict):
    list_of_tables = tables_json["table_names_original"]
    res = ""
    for table in list_of_tables:
        res += make_table_statement(table, schema_dict[table])
        res += "\n\n"
    return res


def make_table_statement(table_name, table):
    statement = "CREATE TABLE `{}` (\n".format(table_name)
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

    parser.add_argument('--gml-file', '-g', dest='gml_file', type=str, required=True,
                        help='GML file')
    parser.add_argument('--output-directory', '-o', dest='output_directory', type=str, required=True,
                        help='Output directory to generate files')
    args = parser.parse_args()

    process(args)
