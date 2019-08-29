"""
Splits given CSV into train, dev and test CSV files
"""

import argparse
import pandas as pd
from pathlib import Path
import subprocess

def prefix_file_path(file_path, prefix):
  file_name = Path(file_path).name
  prefixed_file_name = "%s_%s" % (prefix, file_name)
  return Path(file_path).parent / prefixed_file_name

def print_file_line_count(file_path):
  result = subprocess.run(['wc', '-l', file_path], stdout=subprocess.PIPE)
  print("     > wc -l")
  print(result.stdout.decode())

def write_data_to_file(record_count, data, original_file_path, prefix):
  file_path = prefix_file_path(original_file_path, prefix)
  print("Writing %d records to %s" % (record_count, prefix))
  data.to_csv(path_or_buf=file_path)
  print_file_line_count(file_path)

def main(input_file, test_size, dev_size):
  data = pd.read_csv(input_file)
  data_size = len(data)

  if test_size < 1.0:
    test_size = data_size * test_size
  test_size = int(test_size)

  if dev_size < 1.0:
    dev_size = data_size * dev_size
  dev_size = int(dev_size)

  # shuffle
  data = data.sample(frac=1).reset_index(drop=True)

  if dev_size > 0:
    write_data_to_file(dev_size, data[:dev_size], input_file, 'dev')

  if test_size > 0:
    write_data_to_file(test_size, data[dev_size:dev_size+test_size], input_file, 'test')

  write_data_to_file(data_size-(dev_size+test_size), data[dev_size+test_size:], input_file, 'train')

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--input-file', '-i', dest='input_file', type=str, required=True, help='CSV file to split')
  parser.add_argument('--test-size', '-t', dest='test_size', type=float, required=False, default=0.2, help='Size of test set: float to indicate fraction, or int to indicate count of records, or 0 to skip; default=0.2')
  parser.add_argument('--dev-size', '-d', dest='dev_size', type=float, required=False, default=0.2, help='Size of dev set: float to indicate fraction, or int to indicate count of records, or 0 to skip; default=0.2')
  args = parser.parse_args()

  main(args.input_file, args.test_size, args.dev_size)

# Example usage --
# python csv_split_train_dev_test.py -i ss30_traindev.csv -t 0 -d 0.2

# Example output --
# Writing 179 records to dev
#      > wc -l
#      185 dev_ss30_traindev.csv
#
# Writing 719 records to train
#      > wc -l
#      731 train_ss30_traindev.csv