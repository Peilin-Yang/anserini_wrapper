# -*- coding: utf-8 -*-
import os,sys
import codecs
import subprocess
from subprocess import Popen, PIPE
import shlex
import re
import shutil
import argparse
import json
import csv
import inspect
from multiprocessing import Pool

from search import Search
from performance import Performances

anserini_root = '~/Anserini/'
index_root = '/tuna1/indexes/'
output_root = 'all_results/'
if not os.path.exists(output_root):
    os.makedirs(output_root)

def gen_batch_framework(all_paras, func):
    if len(all_paras) == 0:
        print 'Nothing to run for ' + para_label
        return
    p = Pool(66)
    p.map(func, all_paras)

def gen_run_query_batch():
    all_paras = []
    program = os.path.join(anserini_root, 'target/appassembler/bin', 'SearchCollection')
    suffix = '.pos+docvectors'
    with open('models.json') as mf:
        methods = json.load(mf)
        with open('collections.json') as cf:
            for c in json.load(cf):
                collection_name = c['collection']
                this_output_root = os.path.join(output_root, collection_name)
                if not os.path.exists(this_output_root):
                    os.makedirs(this_output_root)
                index_path = os.path.join(index_root, 'lucene-index.'+collection_name+suffix)
                if not os.path.exists(index_path):
                    index_path += '+rawdocs'
                model_paras = Search(index_path).gen_run_batch_paras('all', methods, this_output_root)
                for para in model_paras:
                    this_para = (
                        program, 
                        '-topicreader', c['topic_reader'],
                        '-index', index_path, 
                        '-topics', ' '.join([os.path.join(anserini_root, 'src/main/resources/topics-and-qrels/', t) for t in c['topic_files']]),
                        para[0],
                        '-output', para[1]
                    )
                    all_paras.append(this_para)
    gen_batch_framework(all_paras, run_query_atom)

def run_query_atom(para):
    subprocess.call(' '.join(para), shell=True)

def gen_output_performances_batch():
    all_paras = []
    with open('collections.json') as cf:
        for c in json.load(cf):
            collection_name = c['collection']
            this_output_root = os.path.join(output_root, collection_name)
            if not os.path.exists(this_output_root):
                os.makedirs(this_output_root)
            index_path = os.path.join(index_root, 'lucene-index.'+collection_name+'.cnt.1')
            all_paras.extend( Performances(index_path).gen_output_performances_paras(this_output_root) )

    #print all_paras
    gen_batch_framework(all_paras, output_performances_atom)


def output_performances_atom(para):
    index_path = para[0]
    output_fn = para[1]
    input_fns = para[2:]
    Performances(index_path).output_performances(output_fn, input_fns)

def print_optimal_performances(metrics=['map']):
    # with open('g.json') as f:
    #     methods = [m['name'] for m in json.load(f)['methods']]
    # if os.path.exists('microblog_funcs.json'):
    #     with open('microblog_funcs.json') as f:
    #         methods.extend([m['name'] for m in json.load(f)['methods']])

    with open('collections.json') as cf:
        for c in json.load(cf):
            collection_name = c['collection']
            this_output_root = os.path.join(output_root, collection_name)
            index_path = os.path.join(index_root, 'lucene-index.'+collection_name+'.cnt.1')
            print 
            print collection_name
            print '='*30
            Performances(index_path).print_optimal_performance(this_output_root, metrics)

def del_method_related_files(method_name):
    folders = ['split_results', 'merged_results', 'evals', 'performances']
    for q in g.query:
        collection_name = q['collection']
        collection_path = os.path.join(_root, collection_name)
        for f in folders:
            if os.path.exists( os.path.join(collection_path, f) ):
                print 'Deleting ' + os.path.join(collection_path, f) + ' *' + method_name + '*'
                if f == 'split_results' or f == 'merged_results':
                    subprocess.call('find %s -name "*method:%s*" -exec rm -rf {} \\;' % (os.path.join(collection_path, f), method_name), shell=True)
                else:
                    subprocess.call('find %s -name "*%s*" -exec rm -rf {} \\;' % (os.path.join(collection_path, f), method_name), shell=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("-b1", "--gen_run_query_batch",
        action='store_true',
        help="Second Step: Generate the batch run query para files")
    parser.add_argument("-b2", "--run_query_atom",
        nargs=1,
        help="Second Step: Run Query")

    parser.add_argument("-e1", "--gen_output_performances_batch",
        action='store_true',
        help="Fifth Step: Generate the performance of each method (for all possible parameters), e.g. best, worst, mean, std")
    parser.add_argument("-e2", "--output_performances_atom",
        nargs=1,
        help="Fifth Step: Generate the performance of each method (for all possible parameters), e.g. best, worst, mean, std")

    parser.add_argument("-del", "--del_method_related_files",
        nargs=1,
        help="Delete all the output files of a method.")

    parser.add_argument("-print_optimal", "--print_optimal_performances",
        nargs='+',
        help="inputs: [evaluation_method]") 

    args = parser.parse_args()

    if args.gen_run_query_batch:
        gen_run_query_batch()
    if args.run_query_atom:
        run_query_atom(args.run_query_atom[0])

    if args.gen_output_performances_batch:
        gen_output_performances_batch()
    if args.output_performances_atom:
        output_performances_atom(args.output_performances_atom[0])

    if args.del_method_related_files:
        del_method_related_files(args.del_method_related_files[0])

    if args.print_optimal_performances:
        print_optimal_performances(args.print_optimal_performances)

