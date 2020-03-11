import argparse
from moz_sql_parser import parse
from moz_sql_parser.formatting import Formatter
import pyparsing
import re
import sys
import pdb

class LogParser:
  def print_error(self, e, text):
    print(e, file=sys.stderr)
    print(file=sys.stderr)
    print("Error parsing query -- ", file=sys.stderr)
    r = int(len(text)/10) + 1
    print("".join([f"{n%10}         " for n in range(0, r)]), file=sys.stderr)
    print("|123456789" * r, file=sys.stderr)
    print(f"[{text}]", file=sys.stderr)
    print('-----------------------------', file=sys.stderr)

  def parse_query(self, text):
    text = text.strip()

    # We don't know where the query ends in given text string
    # So, if parser throws pyparsing.ParseException, we will look at the
    # char index where it errored. Then we will trim string after that point
    # and try again
    try:
      ast = parse(text)
    except pyparsing.ParseException as e:
      error_message = str(e)
      result = re.search(" col\:(\d+)\)", error_message)
      if result:
        char_index = int(result.group(1))
        text = text[:char_index-1].strip()
      else:
        self.print_error(e, text)
        return None

    try:
      ast = parse(text)
    except pyparsing.ParseException as e:
      self.print_error(e, text)
      return None

    query = Formatter().format(ast)
    return query

  def extract_queries(self, file_path):
    with open(file_path, 'r') as f:
      for line in f:
        index = line.find('SELECT ')
        if index > -1:
          text = line[index:]
          if text:
            query = self.parse_query(text)
            if query:
              yield query

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--log_file', '-f', dest='log_file_path', type=str, required=True, help='Path of log file to parse SQL queried from')

  args = parser.parse_args()
  [print(query) for query in LogParser().extract_queries(args.log_file_path)]
