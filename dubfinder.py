import os
import sys
import time
import hashlib
import _thread as thread
from queue import Queue, Empty
from collections import defaultdict

def show_progress(total, processed):
  to_Mb = lambda v: '%.2f Mb' % (v / 10**6)
  steps_count = 50
  progress = dubs.processed // (dubs.total // steps_count)
  perc = '%s/%s' % (to_Mb(total), to_Mb(processed))
  sys.stdout.write('\rProgress: |%s%s| %s' % ('=' * progress, '-' * (steps_count - progress), perc.rjust(30)))
  sys.stdout.flush()

class DubFinder(object):
  MODE_FAST = 'fast'
  MODE_ACCURACY = 'accuracy'

  def __init__(self, path, **kwargs):
    self._processed = 0
    self._total = 0
    self._files_list = []

    self.path = path
    self.mode = kwargs.get('mode', 'fast')
    self.block_size = self._get_bytes(kwargs.get('block_size', '1K'))
    self.min_size = self._get_bytes(kwargs.get('min_size', '0'))
    self.max_size = self._get_bytes(kwargs.get('max_size')) if 'max_size' in kwargs else float('inf')
    self.ext = kwargs.get('ext', '')

    self._read_path()

  def _get_bytes(self, size):
      sufixes = {'K': 10**3, 'M': 10**6, 'G': 10*9, 'T': 10**12}
      factor = 1
      if not size.isdecimal():
        sufix = size[-1:].upper()
        size = size[:-1]
        print(sufix == 'M', sufixes.keys(), sufix not in sufixes.keys())
        if sufix not in sufixes:
          raise ValueError('Incorrect format')
        factor = sufixes[sufix]

      try:
        size = int(size)
      except ValueError:
        raise ValueError('Incorrect format')

      return size * factor

  def _read_path(self):
    for root, _, files in os.walk(self.path):
        for f in files:
          file_path = os.path.join(root, f)
          file_size = os.path.getsize(file_path)
          if self.min_size < file_size < self.max_size:
            self._files_list.append({'file_path': file_path, 'size': file_size})
            self._update_total(file_size)

  def find(self):
    self._processed = 0
    groped = defaultdict(list)
    for line in self._files_list:
      show_progress(self._total, self._processed)
      groped[self._get_hash(line['file_path'])].append(line)
      self._update_processed(line['size'])
    groped = filter(lambda v: len(v) > 1, groped.values())
    return groped

  def _file_read(self, file_path):
    with open(file_path, 'rb') as reader:
      data = reader.read(self.block_size)
      while data:
        data = reader.read(self.block_size)
        yield data
        if self.mode == self.MODE_FAST:
          break

  def _get_hash(self, file_path):
    hs = hashlib.md5()
    for data in self._file_read(file_path):
      hs.update(data)
    return hs.hexdigest()

  def _update_total(self, size):
    size = min(size, self.min_size) if self.min_size and self.mode == self.MODE_FAST else size
    self._total += size

  def _update_processed(self, size):
    size = min(size, self.min_size) if self.min_size and self.mode == self.MODE_FAST else size
    self._processed += size

  # TODO make method eva
  @property
  def processed(self):
    return self._processed

  @property
  def total(self):
    return self._total


if __name__ == '__main__':
  import time
  start = time.time()
  params = {'block_size': '1M', 'min_size': '10M'}
  dubs = DubFinder('/media/mbrutus/3Q/books/', **params)
  finded = dubs.find()
  print('\ntime = %s' % (time.time() - start))
  for dubles in finded:
    for num, duble in enumerate(dubles, 1):
      print('\n%s) %s %sKb' % (num, duble['file_path'], str(duble['size'] / 1000).rjust(30)))
    ui = input()
    if ui == 'q':
      break
    if ui.isdecimal():
      os.unlink(dubles[int(ui) - 1]['file_path'])
