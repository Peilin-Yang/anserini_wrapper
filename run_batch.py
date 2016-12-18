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

import ArrayJob
from search import Search
from performance import Performances

anserini_root = '/home/1471/usr/Anserini/'
index_root = '/lustre/scratch/franklyn/reproduce/collections_lucene/'
output_root = '/lustre/scratch/franklyn/anserini_wrapper/all_results/'
if not os.path.exists(output_root):
    os.makedirs(output_root)

def gen_batch_framework(para_label, batch_pythonscript_para, all_paras, \
        quote_command=False, memory='2G', max_task_per_node=50000, num_task_per_node=50):

    para_dir = os.path.join('batch_paras', '%s') % para_label
    if os.path.exists(para_dir):
        shutil.rmtree(para_dir)
    os.makedirs(para_dir)

    batch_script_root = 'bin'
    if not os.path.exists(batch_script_root):
        os.makedirs(batch_script_root)

    if len(all_paras) == 0:
        print 'Nothing to run for ' + para_label
        return

    tasks_cnt_per_node = min(num_task_per_node, max_task_per_node) if len(all_paras) > num_task_per_node else 1
    all_paras = [all_paras[t: t+tasks_cnt_per_node] for t in range(0, len(all_paras), tasks_cnt_per_node)]
    batch_script_fn = os.path.join(batch_script_root, '%s-0.qs' % (para_label) )
    batch_para_fn = os.path.join(para_dir, 'para_file_0')
    with open(batch_para_fn, 'wb') as bf:
        for i, ele in enumerate(all_paras):
            para_file_fn = os.path.join(para_dir, 'para_file_%d' % (i+1))
            bf.write('%s\n' % (para_file_fn))
            with open(para_file_fn, 'wb') as f:
                writer = csv.writer(f)
                if len(ele) == 1:
                    writer.writerow(ele[0])
                else:
                    writer.writerows(ele)
    command = 'python %s -%s' % (
        inspect.getfile(inspect.currentframe()), \
        batch_pythonscript_para
    )
    arrayjob_script = ArrayJob.ArrayJob()
    arrayjob_script.output_batch_qs_file(batch_script_fn, command, quote_command, True, batch_para_fn, len(all_paras), _memory=memory)
    run_batch_gen_query_command = 'qsub %s' % batch_script_fn
    subprocess.call( shlex.split(run_batch_gen_query_command) )
    """
    for i, ele in enumerate(all_paras):
        batch_script_fn = os.path.join( batch_script_root, '%s-%d.qs' % (para_label, i) )
        batch_para_fn = os.path.join(para_dir, 'para_file_%d' % i)
        with open(batch_para_fn, 'wb') as bf:
            bf.write('\n'.join(ele))
        command = 'python %s -%s' % (
            inspect.getfile(inspect.currentframe()), \
            batch_pythonscript_para
        )
        arrayjob_script = ArrayJob.ArrayJob()
        arrayjob_script.output_batch_qs_file(batch_script_fn, command, quote_command, True, batch_para_fn, len(ele))
        run_batch_gen_query_command = 'qsub %s' % batch_script_fn
        subprocess.call( shlex.split(run_batch_gen_query_command) )
    """

def gen_run_query_batch():
    all_paras = []
    program = os.path.join(anserini_root, 'target/appassembler/bin/SearchWebCollection')
    collection_suffix = ['_nostopwords']
    with open('models.json') as mf:
        methods = json.load(mf)
        with open('collections.json') as cf:
            for c in json.load(cf):
                collection_name = c['collection']
                for suffix in collection_suffix:
                    this_output_root = os.path.join(output_root, collection_name+suffix)
                    if not os.path.exists(this_output_root):
                        os.makedirs(this_output_root)
                    index_path = os.path.join(index_root, collection_name+suffix)
                    model_paras = Search(index_path).gen_run_batch_paras(methods, this_output_root)
                    for para in model_paras:
                        this_para = (
                            program, 
                            '-topicreader', c['topic_reader'], 
                            '-index', index_path, 
                            '-topics', ' '.join([os.path.join(anserini_root, 'src/main/resources/topics-and-qrels/', t) for t in c['topic_files']]),
                            para[0],
                            '-output', os.path.join(this_output_root, para[1]),
                            '-eval', '-evalq', '-qrels', 
                            ' '.join([os.path.join(anserini_root, 'src/main/resources/topics-and-qrels/', t) for t in c['qrels']]),
                            '-evalo', os.path.join(this_output_root, para[2]),
                        )
                        all_paras.append(this_para)
    gen_batch_framework('run_anserini_queries', 'b2', all_paras)

def run_query_atom(para_file):
    with open(para_file) as f:
        reader = csv.reader(f)
        for row in reader:
            subprocess.call(' '.join(row), shell=True)

def gen_output_performances_batch(eval_method='map'):
    all_paras = []
    collection_suffix = ['_nostopwords']
    with open('collections.json') as cf:
        for c in json.load(cf):
            collection_name = c['collection']
            for suffix in collection_suffix:
                this_output_root = os.path.join(output_root, collection_name+suffix)
                if not os.path.exists(this_output_root):
                    os.makedirs(this_output_root)
                index_path = os.path.join(index_root, collection_name+suffix)
                all_paras.extend( Performances(index_path).gen_output_performances_paras(this_output_root) )

    #print all_paras
    gen_batch_framework('gen_performances', 'e2', all_paras)


def output_performances_atom(para_file):
    with open(para_file) as f:
        reader = csv.reader(f)
        for row in reader:
            index_path = row[0]
            model_name = row[1]
            output_fn = row[2]
            input_fns = row[3:]
            Performances(index_path).output_performances(output_fn, input_fns)

def output_the_optimal_performances(eval_method='map'):
    # with open('g.json') as f:
    #     methods = [m['name'] for m in json.load(f)['methods']]
    # if os.path.exists('microblog_funcs.json'):
    #     with open('microblog_funcs.json') as f:
    #         methods.extend([m['name'] for m in json.load(f)['methods']])

    for q in g.query:
        collection_name = q['collection']
        collection_path = os.path.join(_root, collection_name)
        print 
        print collection_name
        print '='*30
        for q_part in q['qf_parts']:
            print q_part
            print '-'*30
            Performances(collection_path).print_optimal_performance(eval_method, q_part)

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

    parser.add_argument("-output-optimal", "--output_the_optimal_performances",
        nargs=1,
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

    if args.output_the_optimal_performances:
        output_the_optimal_performances(args.output_the_optimal_performances[0])

