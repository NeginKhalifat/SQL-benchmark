import os
import re
import asyncio

import sqlite3
import threading
from typing import Tuple, Any, List, Set
from itertools import product
from collections import defaultdict
import tqdm
import random
from parse import get_all_preds_for_execution, remove_distinct
import time
import pickle as pkl
import subprocess
from itertools import chain



threadLock = threading.Lock()
TIMEOUT = 60
EXEC_TMP_DIR = 'tmp/'

def permute_tuple(element: Tuple, perm: Tuple) -> Tuple:
    assert len(element) == len(perm)
    return tuple([element[i] for i in perm])


def unorder_row(row: Tuple) -> Tuple:
    return tuple(sorted(row, key=lambda x: str(x) + str(type(x))))


# unorder each row in the table
# [result_1 and result_2 has the same bag of unordered row]
# is a necessary condition of
# [result_1 and result_2 are equivalent in denotation]
def quick_rej(result1: List[Tuple], result2: List[Tuple], order_matters: bool) -> bool:
    s1 = [unorder_row(row) for row in result1]
    s2 = [unorder_row(row) for row in result2]
    if order_matters:
        return s1 == s2
    else:
        return set(s1) == set(s2)


# return whether two bag of relations are equivalent
def multiset_eq(l1: List, l2: List) -> bool:
    if len(l1) != len(l2):
        return False
    d = defaultdict(int)
    for e in l1:
        d[e] = d[e] + 1
    for e in l2:
        d[e] = d[e] - 1
        if d[e] < 0:
            return False
    return True


def get_constraint_permutation(tab1_sets_by_columns: List[Set], result2: List[Tuple]):
    num_cols = len(result2[0])
    perm_constraints = [{i for i in range(num_cols)} for _ in range(num_cols)]
    if num_cols <= 3:
        return product(*perm_constraints)

    # we sample 20 rows and constrain the space of permutations
    for _ in range(20):
        random_tab2_row = random.choice(result2)

        for tab1_col in range(num_cols):
            for tab2_col in set(perm_constraints[tab1_col]):
                if random_tab2_row[tab2_col] not in tab1_sets_by_columns[tab1_col]:
                    perm_constraints[tab1_col].remove(tab2_col)
    return product(*perm_constraints)

def tuple_subset(t1: Tuple, t2: Tuple) -> bool:
    """
    Check if all elements of t1 appear in t2 in the same order, even if t2 has extra elements.
    """
    it = iter(t2)
    return all(any(elem == item for item in it) for elem in t1)

# check whether two denotations are correct
def result_eq(result1: List[Tuple], result2: List[Tuple], order_matters: bool, partial_correct =False) -> bool:
    # print("res1", result1)
    # print("res2", result2)
    # print(type(result1))

    # print(type(result2))
    if type(result1)!=type(result2):
        # print("*****%")
        return False

    
    if len(result1) == 0 and len(result2) == 0:
        return True

    # if length is not the same, then they are definitely different bag of rows
    if len(result1) != len(result2):
       
        return False
    num_cols = len(result1[0])

    # if the results do not have the same number of columns, they are different
    if len(result2[0]) != num_cols:
         # todo partial answers
    
        return False

    # unorder each row and compare whether the denotation is the same
    # this can already find most pair of denotations that are different
    if not quick_rej(result1, result2, order_matters):
        return False

    # the rest of the problem is in fact more complicated than one might think
    # we want to find a permutation of column order and a permutation of row order,
    # s.t. result_1 is the same as result_2
    # we return true if we can find such column & row permutations
    # and false if we cannot
    tab1_sets_by_columns = [{row[i] for row in result1} for i in range(num_cols)]

    # on a high level, we enumerate all possible column permutations that might make result_1 == result_2
    # we decrease the size of the column permutation space by the function get_constraint_permutation
    # if one of the permutation make result_1, result_2 equivalent, then they are equivalent
    for perm in get_constraint_permutation(tab1_sets_by_columns, result2):
        if len(perm) != len(set(perm)):
            continue
        if num_cols == 1:
            result2_perm = result2
        else:
            result2_perm = [permute_tuple(element, perm) for element in result2]
        if order_matters:
            if result1 == result2_perm:
                return True
        else:
            # in fact the first condition must hold if the second condition holds
            # but the first is way more efficient implementation-wise
            # and we use it to quickly reject impossible candidates
            if set(result1) == set(result2_perm) and multiset_eq(result1, result2_perm):
                return True
    return False


def replace_cur_year(query: str) -> str:
    return re.sub(
        "YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*", "2020", query, flags=re.IGNORECASE
    )


# get the database cursor for a sqlite database path
def get_cursor_from_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor


async def exec_on_db_(sqlite_path: str, query: str) -> Tuple[str, Any]:
    query = replace_cur_year(query)
    cursor = get_cursor_from_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return "result", result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return "exception", e

async def exec_on_db(
    sqlite_path: str, query: str, process_id: str = "", timeout: int = TIMEOUT
) -> Tuple[str, Any]:
    try:
        return await asyncio.wait_for(exec_on_db_(sqlite_path, query), timeout)
    except asyncio.TimeoutError:
        return ('exception', TimeoutError)
    except Exception as e:
        return ("exception", e)


# postprocess the model predictions to avoid execution errors
# e.g. removing spaces between ">" and "="
def postprocess(query: str) -> str:
    query = query.replace('> =', '>=').replace('< =', '<=').replace('! =', '!=')
    return query


# approximate whether p_str and g_str are semantically equivalent
# db is the database path
# we are going to evaluate whether they are equivalent in all the databases
# that are in the same directory as db
# 0 if denotationally equivalent
# 1 otherwise
# the meaning of each auxillary argument can be seen in the parser definition in evaluation.py
def eval_exec_match(db: str, p_str: str, g_str: str, plug_value: bool, keep_distinct: bool, progress_bar_for_each_datapoint: bool) -> int:
    # post-process the prediction.
    # e.g. removing spaces between ">" and "="
    if p_str =="Not Found":
        # print("HHHH")
        return 0
    # print("g_str", g_str)
    # print("p_str", p_str)
    p_str, g_str = postprocess(p_str), postprocess(g_str)
    # print("PPPPstar:", g_str)
    # print("GGG",g_str )
    if not keep_distinct:
        p_str = remove_distinct(p_str)
        g_str = remove_distinct(g_str)
    # print(g_str)

    # we decide whether two denotations are equivalent based on "bag semantics"
    # https://courses.cs.washington.edu/courses/cse444/10sp/lectures/lecture16.pdf
    # if there is order by in query, then we assume order of the rows matter
    # order by might also be used to find the max/min instead of sorting,
    # but in that case the result mostly only contains one row and hence order_matters does not make a difference
    order_matters = 'order by' in g_str.lower()

    # find all databases in the same directory
    db_dir = os.path.dirname(db)
    db_paths = [os.path.join(db_dir, basename) for basename in os.listdir(db_dir) if '.sqlite' in basename]

    preds = [p_str]
    # if plug in value (i.e. we do not consider value prediction correctness)
    # enumerate all ways to plug in values in the gold query to the model predictions
    # otherwise, we only evaluate the predicted query with its own value prediction
    if plug_value:
        _, preds = get_all_preds_for_execution(g_str, p_str)
        # we did not add this line in our EMNLP work
        # this reduces "false negatives" when value is substituted
        preds = chain([p_str], preds)

    for pred in preds:
        # print("HI1", pred)
        partial_correct = False
        pred_passes = 1
        # compare the gold and predicted denotations on each database in the directory
        # wrap with progress bar if required
        if progress_bar_for_each_datapoint:
            ranger = tqdm.tqdm(db_paths)
        else:
            ranger = db_paths
        # print(ranger)
        for db_path in ranger:
            # print(exec_on_db(db_path, pred))
            g_flag, g_denotation = asyncio.run(exec_on_db(db_path, g_str))
            p_flag, p_denotation = asyncio.run(exec_on_db(db_path, pred))
            
            # print("p_denotation: ", p_denotation)
            # print("g_denotation: ", g_denotation)
            # print("p-flag: ",p_flag)
            # print("Pred: ",pred)

            # print("g_str: ", g_str)
            # print("((((((()))))))")

            # we should expect the gold to be succesfully executed on the database
            assert g_flag != 'exception', 'gold query %s has error on database file %s' % (g_str, db_path)

            # wrong if execution fails
            if p_flag == 'exception':
                # print("EXCEPTION")
                pred_passes = 0
            # if g_denotation == [] and p_denotation == []:
                # print("#############################")
                # print("Both are empty")
                # print("GT: ",g_str)
                # print("Pred: ",pred)
                # print("#############################")

            # if denotations are not equivalent, the prediction must be wrong
            elif not result_eq(g_denotation, p_denotation, order_matters=order_matters):
                # print("p_notation", p_denotation)
                # print("g_notation",g_denotation )
                if type(g_denotation)!= type(p_denotation):
                    # print("HIII")
                    pred_passes = 0
                    break

                if len(g_denotation)>0 and  len(p_denotation)>0 and len(g_denotation[0]) != len(p_denotation[0]):
                    # print("WHY")
         # todo partial answers
        # can we remove one column to make them the same?
       

    # Check for partial answers: if result1 is a subset of result2 or vice versa
                    for r1 in g_denotation:
                        
                        # print("r1",r1)
                        # print(result2)
                        if not any(tuple_subset(r1, r2) for r2 in p_denotation):
                            return False
                    partial_correct = True
                    print("Partial answer",partial_correct)
                    
                    if  partial_correct:
                        print("---------------------")
                        print("Not equal but Partially Correct:")
                        print(g_str)
                        print(pred)
                        print("---------------------")

                pred_passes = 0
            if pred_passes == 0:
                break

        # the model prediction has the same denotation as the gold for all databases
        if pred_passes == 1:
            return 1

    # none of the predictions passed
    return 0


S1 = [(5, 99999998, 'Tyler Swift', 'ts@superstar.com\n'), (35425845, 3, 'Black Widow\n', 'bw@superhero.com'), 
      (1, 6662425, 'Iron Man', 'ts@richest.com'), (2, 890, 'Mary', 'Mary@yale.edu'), (1, 4, 'Susan', 'susan@gmail.com\n')]

S2 = [(10, 5, 99999998, 'Tyler Swift', 'ts@superstar.com\n'), (23, 35425845, 3, 'Black Widow\n', 'bw@superhero.com'), 
      (1, 1, 6662425, 'Iron Man', 'ts@richest.com'), (2, 2, 890, 'Mary', 'Mary@yale.edu'), (100, 1, 4, 'Susan', 'susan@gmail.com\n')]

print(result_eq(S1, S2, False))  # Should print True for partial correctness