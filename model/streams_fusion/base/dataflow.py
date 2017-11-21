# coding=utf-8
import os
from random import choice
import numpy as np
from StringIO import StringIO
import subprocess
import requests
import pandas as pd


class CLI:
    def __init__(self, user, HADOOP_HOME, url):
        self.user = user
        if HADOOP_HOME is not None:
            self.HADOOP_HOME = HADOOP_HOME
        elif url == 'localhost':
            self.HADOOP_HOME = os.environ['HADOOP_HOME']
        else:
            self.HADOOP_HOME = None
        self.url = url

    def get_output(self, cmd):
        if self.url == 'localhost':
            cmd = 'export HADOOP_ROOT_LOGGER=WARN,DRFA\n sudo -u{user} {HADOOP_HOME}/bin/hadoop {cmd}'.format(
                user=self.user,
                HADOOP_HOME=self.HADOOP_HOME,
                cmd=cmd
            )
            s = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = s.communicate()
            status = s.returncode
            output = stdout.strip()
        else:
            params = {
                'cmd': cmd,
                'HADOOP_HOME': str(self.HADOOP_HOME),
                'user': self.user
            }
            respond = requests.get(self.url, params=params).json()
            status = respond['status']
            output = respond['output']
        return output if status is 0 else None


class HDFS:
    def __init__(self, path, cli_url='http://l-qdws6.tc.cn1.qunar.com:12306', user='bizdata', HADOOP_HOME=None):
        self.path = path
        self.cli = CLI(user=user, HADOOP_HOME=HADOOP_HOME, url=cli_url)

        self.is_path_dir = 0 if self.cli.get_output('fs -test -d %s' % path) is None else 1

        self.file_name_list = map(lambda x: x.split(' ')[-1], self.cli.get_output('fs -ls -R %s' % path).split('\n'))
        self.max_file_size = 4 * 1024 * 1024 * 1024
        self.file_size = int(self.cli.get_output('fs -du -s %s' % path).split(' ')[0])

        self.data = None

    def get(self):
        if self.file_size < self.max_file_size:
            if self.is_path_dir:
                table = self.cli.get_output('fs -cat %s/*' % self.path)
            else:
                table = self.cli.get_output('fs -cat %s' % self.path)
            # self.data = np.genfromtxt(StringIO(table), dtype=np.string_)
            self.data = pd.read_table(StringIO(table)).as_matrix()
        else:
            raise Exception('hdfs文件当前大小(%d byte) > 支持最大(%d byte), 请使用 get_batch 获取'
                            % (self.file_size, self.max_file_size))
        return self

    def get_batch(self, batch_size=None, order='rand'):
        if order == 'seq':
            if batch_size is not None: raise Exception('当有序地获取batch时不支持batch_size')
            filename = None if not self.file_name_list else self.file_name_list.pop(0)
            table = self.cli.get_output('fs -cat %s' % filename) if filename is not None else None
        elif order == 'rand':
            filename = choice(self.file_name_list)
            shuffle = "awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}'"
            limit = "head -%d " % (batch_size or 1024)
            table = self.cli.get_output('fs -cat {filename} | {shuffle} | {limit}'.format(
                filename=filename,
                shuffle=shuffle,
                limit=limit
            ))
        else:
            raise Exception('不支持 order=%s 的取数方式，只支持rand/seq')
        # self.data = np.genfromtxt(StringIO(table), dtype=np.string_) if table is not None else None
        self.data = pd.read_table(StringIO(table)).as_matrix() if table is not None else None
        return self

    def except_col(self, cols):
        if self.data is None: return self
        if isinstance(cols, int): cols = [cols]
        if isinstance(cols, slice): cols = range(self.data.shape[1])[cols]
        needed_col = [i for i in range(self.data.shape[1]) if i not in cols]
        self.data = self.data[:, needed_col]
        return self

    def only_col(self, cols):
        if self.data is None: return self
        if isinstance(cols, int): cols = [cols]
        self.data = self.data[:, cols]
        return self

    def split_to(self, *cols):
        self.data = [None for _ in cols] if self.data is None else [self.data[:, col] for col in cols]
        return self

    def as_ndarray(self, *dtypes):
        typemap = {'str': np.str_, 'int': np.int32, 'double': np.double, 'string': np.str_, 'float32': np.float32}
        typelist = [typemap.get(dtype) for dtype in dtypes] or [np.double]
        if self.data is None:
            return None
        elif isinstance(self.data, list):
            return [None if data is None else np.array(data, dtype=dtype) for data, dtype in zip(self.data, typelist)]
        else:
            return np.array(self.data, dtype=typelist[0])
