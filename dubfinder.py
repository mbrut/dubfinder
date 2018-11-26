#!/usr/bin/env python3

import re
import os
import sys
import time
import hashlib
import argparse
from collections import defaultdict

class SizeParser(object):
    SUFIXES = {'K': 10**3, 'M': 10**6, 'G': 10**9, 'T': 10**12}

    @classmethod
    def to_bytes(cls, size):
      sufixes = cls.SUFIXES.keys()
      base, factor = re.match(r'(\d+)?(%s)?' % '|'.join(sufixes), str(size), re.I).groups()
      try:
        base = int(base)
        factor = factor and cls.SUFIXES[factor.upper()] or 1 
      except (TypeError, ValueError, KeyError):
        raise ValueError('Incorrect value?')
      
      return base * factor
      
class DubFinder(object):
  MODE_FAST = 'fast'
  MODE_ACCURACY = 'accuracy'

  def __init__(self, path, **kwargs):
    self.finded = []
    
    self.path = path
    val_or_default = lambda name, default: kwargs.get(name) or default
    self.mode = val_or_default('block_size', self.MODE_FAST)
    self.block_size = SizeParser.to_bytes(val_or_default('block_size', '1M'))
    self.min_size = SizeParser.to_bytes(val_or_default('min_size', 0))
    self.max_size = SizeParser.to_bytes(kwargs.get('max_size')) if kwargs.get('max_size') else float('inf')
    self.ext = val_or_default('ext', [])

  def process_files(self):
    gropped = defaultdict(list)
    for line in self._read_path():
      gropped[self._get_file_digest(line['file_path'])].append(line)
      yield line['file_path']
    self.finded = filter(lambda v: len(v) > 1, gropped.values())

  def _read_path(self):
    for root, _, files in os.walk(self.path):
        for f in files:
          file_path = os.path.join(root, f)
          file_size = os.path.getsize(file_path)
          correct_size = self.min_size < file_size < self.max_size
          correct_ext = os.path.splitext(file_path)[1].lstrip('.') in self.ext if self.ext else True
          if correct_size and correct_ext:
            yield {'file_path': file_path, 'size': file_size}

  def _get_file_digest(self, file_path):
    hs = hashlib.md5()
    for data in self._file_read(file_path):
      hs.update(data)
    return hs.hexdigest()

  def _file_read(self, file_path):
    with open(file_path, 'rb') as reader:
      while True:
        data = reader.read(self.block_size)
        yield data
        if self.mode == self.MODE_FAST:
          break


def get_range(text_range):
  total_range = set()
  for range_part in map(str.strip, text_range.split(',')):
    range_part = list(map(parse_int, range_part.split('-')))
    if len(range_part) == 0 or len(range_part) > 2 or not all(range_part):
      return None
    if len(range_part) == 1:
      total_range = total_range | set(range_part)
    if len(range_part) == 2:
        total_range = total_range | set(range(range_part[0], range_part[1] + 1)) 
  return total_range

def parse_int(digit):
  try:
    return int(digit)
  except ValueError:
    return None

def process_user_input():
  ui = input('Type range, ex. 1, 2, 5-7\nEnter to show next files\nq to Exit\n')
  if ui == 'q':
    return 'exit'
  if ui == '':
    return 'continue'
  remove_range = get_range(ui)
  if not remove_range:
    print('Invalid input, try again')
    return process_user_input()
  deleted_files = []
  for remove_num in remove_range:
    remove_file = dubles[remove_num - 1]['file_path']
    deleted_files.append(remove_file)
    os.unlink(remove_file)
  return deleted_files


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('path', help='Search directory')
  parser.add_argument('--min_size', help='Min file size')
  parser.add_argument('--max_size', help='Max file size')
  parser.add_argument('--block_size', help='Size block in fast mode')
  parser.add_argument('-e', '--ext', action='append', default=[], help='Extention list')
  parser.add_argument('-f', '--fast', action='store_const', const=DubFinder.MODE_FAST, help='Fast mode, best for big binary files')
  parser.add_argument('-a', '--accuracy', action='store_const', const=DubFinder.MODE_ACCURACY, help='Accuracy mode, best for text files')
  parser.add_argument('-v', '--verbose', action='store_true', help='Show current process file')
  args = parser.parse_args()

  params = {'mode': args.accuracy or args.fast or 'fast', 'block_size': args.block_size, 
            'ext': args.ext, 'min_size': args.min_size, 'max_size': args.max_size}
  dubs = DubFinder(args.path, **params)
  for current_file in dubs.process_files():
    if args.verbose:
      print('Processed: %s' % current_file)

  for dubles in dubs.finded:
    for num, duble in enumerate(dubles, 1):
      print('\n%s) %s(%sKb)' % (num, duble['file_path'], str(duble['size'] / 1000)))

    ui = process_user_input()
    if ui == 'exit':
      break
    elif ui == 'continue':
      continue
    else:
      print('Deleted:\n%s' % '\n'.join(ui))
      input('Enter to continue')
