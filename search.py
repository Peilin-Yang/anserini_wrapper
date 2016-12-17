# -*- coding: utf-8 -*-
import sys,os
import itertools
from inspect import currentframe, getframeinfo
from subprocess import Popen, PIPE
import numpy as np

reload(sys)
sys.setdefaultencoding('utf-8')

class Search(object):
    def __init__(self, collection_path):
        self.corpus_path = os.path.abspath(collection_path)
        if not os.path.exists(self.corpus_path):
            frameinfo = getframeinfo(currentframe())
            print frameinfo.filename, frameinfo.lineno
            print '[Evaluation Constructor]:Please provide a valid corpus path'
            exit(1)

        self.run_files_root = os.path.join(self.corpus_path, 'run_files')

    def gen_run_batch_paras(self, methods):
        all_paras = []
        if not os.path.exists(self.run_files_root):
            os.makedirs(self.run_files_root)

        for m in methods:
            if 'paras' in m:
                print [np.arange(ele[0], ele[1], ele[2]).tolist() for ele in m['paras'].values()]
                print '-'*40
                for p in itertools.product(*[np.arange(ele[0], ele[1], ele[2]).tolist() for ele in m['paras'].values()]):
                    para_str = '-%s' % m['name']
                    rfn = m['name']+'-'
                    for k_idx, k in enumerate(m['paras'].keys()):
                        para_str += ' -%s %s' % (k, p[k_idx])
                        if k_idx != 0:
                            rfn += ','
                        rfn += '%s:%s' % (k, p[k_idx])
                    results_fn = os.path.join(self.run_files_root, rfn)
            else:
                para_str = '-%s' % m['name']
                results_fn = os.path.join(self.run_files_root, m['name'])

            if not os.path.exists(results_fn):
                all_paras.append( (para_str, results_fn) )
        return all_paras
        