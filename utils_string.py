import re

def safe_key_string(key: str) -> str:
  # change non-alphanumeric characters to _
  key = re.sub(r'[^a-zA-Z0-9]', '_', key)
  # add _ if key has leading numbers
  if re.match(r'^[0-9]', key):
    key = '_' + key
  return key

def safely_split(str, delimiter=' ', expected_len=-1):
  '''
  Safely split a string by delimiter
  '''
  splited = str.split(delimiter, expected_len)
  if expected_len > 0 and len(splited) != expected_len:
    # convert to expected_len
    splited = splited + [''] * (expected_len - len(splited))

  return splited
