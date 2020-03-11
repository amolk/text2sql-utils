import pytest
from ..log_parser import LogParser

def test_class_exists():
  assert LogParser
  assert LogParser()

def test_single_line_queries():
  queries = list(LogParser().extract_queries('tests/fixtures/single_line_queries.log'))
  assert len(queries) == 2
  assert queries[0] == 'SELECT id, name FROM geography_postal_codes WHERE state_ids IS NOT NULL ORDER BY space_count DESC'
  assert queries[1] == "SELECT * FROM geometry_columns WHERE f_table_name = 'geography_neighborhoods'"