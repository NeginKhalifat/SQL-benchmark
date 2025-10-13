import json
import sqlite3
from nltk import word_tokenize
import importlib
# evaluation = importlib.import_module("evaluation")
from query_generation.read_schema.read_schema import convert_json_to_schema
import copy
# tsa = importlib.import_module("test-suite-sql-eval-master.evaluation")
# evaluator= evaluation.Evaluator()

import re
CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'having', 'order','limit', 'intersect', 'union', "union_all", 'except', 'offset')
JOIN_KEYWORDS = ('join', 'on', 'as')
JOIN_TYPES = ('join', "inner",'left', 'right', 'full', 'cross') 

WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists', "<>")
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg','length', 'abs', 'upper', 'lower')
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}
STR_NUM_FUNC = ('length', 'abs', 'upper', 'lower')

COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except', "union_all")
ORDER_OPS = ('desc', 'asc')

KEYWORDS = CLAUSE_KEYWORDS + JOIN_KEYWORDS + WHERE_OPS + UNIT_OPS + AGG_OPS + COND_OPS + SQL_OPS + ORDER_OPS + JOIN_TYPES + ("having",)


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema):
        self._schema = schema
        self._idMap = self._map(self._schema)
        # ##############print(self._idMap)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema):
        idMap = {'*': "__all__"}
        id = 1
        for key, vals in schema.items():
            for val in vals:
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1

        for key in schema:
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1

        return idMap


def get_schema(db):
    """
    Get database's schema, which is a dict with table name as key
    and list of column names as value
    :param db: database path
    :return: schema dict
    """

    schema = {}
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # fetch table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # fetch table info
    for table in tables:
        cursor.execute("PRAGMA table_info({})".format(table))
        schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema


def get_schema_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)

    schema = {}
    for entry in data:
        table = str(entry['table'].lower())
        cols = [str(col['column_name'].lower()) for col in entry['col_data']]
        schema[table] = cols

    return schema


def tokenize(string):
    string = str(string)
    # string = string.replace("''", "__escaped_single_quote__")
    string = string.replace("''", "__empty_string__")

    # Replace single-quoted strings with double-quoted equivalents
    # Escaped quotes (__escaped_single_quote__) are converted back to single quotes
    def replace_quotes(match):
        content = match.group(1).replace("__empty_string__", "").replace("''", "'")
        return f'"{content}"'

    # Match single-quoted strings and replace them
    string = re.sub(r"'((?:[^']|'')*)'", replace_quotes, string)
    #####print("After handling single-quoted strings:", string)
    quote_idxs = [idx for idx, char in enumerate(string) if char == '"']
    assert len(quote_idxs) % 2 == 0, "Unexpected quote"
    string = re.sub(r'\bunion\s+all\b', 'union_all', string, flags=re.IGNORECASE)
    # if not quote_idxs:  # If no quotes are found, skip further processing of quotes
    #     #####print("No quotes found. Proceeding without handling string values.")
    #     string = string.replace("__empty_string__", "''")  # Restore empty strings early
       

    # keep string value as token
    vals = {}
    for i in range(len(quote_idxs)-1, -1, -2):
        #####print("HIII")
        qidx1 = quote_idxs[i-1]
        qidx2 = quote_idxs[i]
        val = string[qidx1: qidx2+1]
        key = "__val_{}_{}__".format(qidx1, qidx2)
        string = string[:qidx1] + key + string[qidx2+1:]
        # vals[key] = val
        #####print(val)
        # vals[key] = val.replace("__empty_string__", "''")  # Restore escaped single quotes
        vals[key] = val
    string = re.sub(r'([+\-*/<>!=])', r' \1 ', string)
    string = re.sub(r'(?<![\w)])\s*-\s*(\d+(\.\d+)?)', r' -\1', string)  # Attach '-' to negative numbers


    # string = re.sub(r'\bnot\s+exists\b', 'not_exists', string, flags=re.IGNORECASE)
    # string = re.sub(r'\bnot\s+in\b', 'not_in', string, flags=re.IGNORECASE)
        # string = re.sub(r'(between.*?and)', r' \1 ', string, flags=re.IGNORECASE)

    toks = [word.lower() for word in word_tokenize(string)]
    # ##############print("FIRST: ",toks)
    # replace with string value token
    for i in range(len(toks)):
        if toks[i] in vals:
            toks[i] = vals[toks[i]]
    toks = [tok.replace("__empty_string__", "''") for tok in toks]

    # find if there exists !=, >=, <=
    eq_idxs = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    gt_idxs = [idx for idx, tok in enumerate(toks) if tok == ">"]
    gt_idxs.reverse()
    pref_gt = "<"
    prefix = ('!', '>', '<')
    for eq_idx in eq_idxs:
        pre_tok = toks[eq_idx-1]
        if pre_tok in prefix:
            toks = toks[:eq_idx-1] + [pre_tok + "="] + toks[eq_idx+1: ]
    for gt_idx in gt_idxs:
        pre_tok = toks[gt_idx-1]
        if pre_tok == pref_gt:
            toks = toks[:gt_idx-1] + [pre_tok +toks[gt_idx] ] + toks[gt_idx+1: ]
    #####print(toks)
    return toks



def scan_alias(toks):
    """Scan the index of 'as' and build the map for all alias (both table and column)"""
    as_idxs = [idx for idx, tok in enumerate(toks) if tok == 'as']
    alias = {}
    for idx in as_idxs:
        prev_tokes = toks[idx-1]
        temp = toks[idx-2]  
        if prev_tokes ==")":
            subquery_start = idx-1

            open_parens = 1
            while subquery_start > 0:
                subquery_start -= 1
                if toks[subquery_start] == ')':
                    open_parens += 1
                elif toks[subquery_start] == '(':
                    open_parens -= 1
                if open_parens == 0:
                    break
            if toks[idx+1] in alias:
                temp = alias[toks[idx+1]]
                alias[toks[idx+1]] =[]
                alias[toks[idx+1]].append(temp)
                alias[toks[idx+1]].append( toks[subquery_start:idx])
            else:  
            #############print("SUBQUEY START",toks[subquery_start:idx-1])
                alias[toks[idx+1]] = toks[subquery_start:idx]
          
            ###########print("TT",alias)

        else:
            while temp not in KEYWORDS and temp!="," and temp!="as" and temp not in alias and temp not in JOIN_KEYWORDS:  
                prev_tokes = temp + " " + prev_tokes
                temp = toks[toks.index(temp)-1]
            if toks[idx+1] in alias:
                temp = alias[toks[idx+1]]
                alias[toks[idx+1]] =[]
                alias[toks[idx+1]].append(temp)
                alias[toks[idx+1]].append( prev_tokes)
            else:  
                alias[toks[idx+1]] = prev_tokes
                ###########print("TT", alias)

    # ###print("ALIAS with AS: ",alias)
    #Handle aliases without 'as'
    for i in range(len(toks)):
        # ##print(toks[i])
        
        if toks[i] not in KEYWORDS and toks[i]!=")" and toks[i]!= "," and toks[i - 1] not in KEYWORDS and toks[i - 1]!="("and toks[i - 1]!="," and toks[i-1]!="distinct" and toks[i-1]!="by" and toks[i]!="(" and toks[i]!=";":
            # ##print("HI")
            # ##print(toks[i])
            prev_tokes = toks[i-1]
            # ##########print(prev_tokes)

            if prev_tokes in alias:
                assert False, "Error alias name: {}".format(toks[i])

            temp = toks[i-2]   
            while temp not in KEYWORDS and temp!=",":  
                prev_tokes = temp + " " + prev_tokes
                temp = toks[toks.index(temp)-1]
            if toks[i] in alias:
                temp = alias[toks[i]]
                alias[toks[i]] =[]
                alias[toks[i]].append(temp)
                alias[toks[i]].append( prev_tokes)
            else:  
                alias[toks[i]] = prev_tokes
                ###########print("TT",alias)
    # ###print("ALIAS with/without AS: ",alias)
    return alias

def get_tables_with_alias(schema, toks):
    # ########print(toks)
    tables = scan_alias(toks)
    # #############print(tables)
    for key in schema:
        # #############print(key)
        # assert key not in tables, "Alias {} has the same name in table".format(key)
        if key not in tables:
            tables[key] = key
    # ##########print(tables)
    return tables


def parse_col(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
        :returns next idx, column id
    """
    tok = toks[start_idx]
    # #print("^^^:", tok)
    flag = True
    # #print(tables_with_alias)
    for alias in default_tables:
        table = tables_with_alias[alias]
        if tok in schema.schema[table]:
            flag =False
    # #print(flag)
    if tok == "*":
        return start_idx + 1, schema.idMap[tok], False
    if tok in tables_with_alias and flag:
        return start_idx+1, tables_with_alias[tok], True
    ###########print(tables_with_alias)
    if '.' in tok:  # if token is a composite
        alias, col = tok.split('.')
        


        if isinstance(tables_with_alias[alias], dict) and tables_with_alias[alias].get("type") == "subquery":
                # Subquery alias: resolve column in subquery's output
                for sub_col in tables_with_alias[alias]["columns"]:
                    if col in sub_col:
                        #############print("COL: ", col)
                        return start_idx + 1, f"{alias}.{col}", False
                raise ValueError(f"Column '{col}' not found in subquery alias '{alias}'.")
        elif isinstance(tables_with_alias[alias], list):
            # ###########print("YAMM", tables_with_alias[alias])
            # ###########print(col)
            for table in tables_with_alias[alias]:
                try:
                    key = table + "." + col
                    # ###########print(key)
                    return start_idx+1, schema.idMap[key], False
                except:
                     if isinstance(tables_with_alias[alias], dict) and tables_with_alias[alias].get("type") == "subquery":
                # Subquery alias: resolve column in subquery's output
                        for sub_col in tables_with_alias[alias]["columns"]:
                            if col in sub_col:
                                #############print("COL: ", col)
                                return start_idx + 1, f"{alias}.{col}", False
                        raise ValueError(f"Column '{col}' not found in subquery alias '{alias}'.")


            if col in tables_with_alias:
                return start_idx+1, tables_with_alias[col], True

            if col in tables_with_alias[alias]:

                key = tables_with_alias[alias] + "." + col
                # ###########print("HIJII")

                

            
        else:
            key = tables_with_alias[alias] + "." + col

        ##############print("KKKKK",key)
        #############print("++++++++++: ", schema.idMap[key])
        return start_idx+1, schema.idMap[key], False

    assert default_tables is not None and len(default_tables) > 0, "Default tables should not be None or empty"
    
    for alias in default_tables:
        table = tables_with_alias[alias]
        if tok in schema.schema[table]:
            key = table + "." + tok
            return start_idx+1, schema.idMap[key], False

    assert False, "Error col: {}".format(tok)


def parse_col_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
        :returns next idx, (agg_op id, col_id)
    """
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    isDistinct = False
    ##print("col", toks[idx])
    if toks[idx] == '(':
        isBlock = True
        idx += 1
    #########print(toks[idx])
    agg_id = AGG_OPS.index("none")

    if toks[idx] in AGG_OPS:
        # #########print(toks[idx])

        if toks[idx+1]!="(":
            agg_id = AGG_OPS.index("none")
        else:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
            assert idx < len_ and toks[idx] == '('
            idx += 1
        if toks[idx] == "distinct":
            idx += 1
            isDistinct = True
        
        idx, col_id, replace_col = parse_col(toks, idx, tables_with_alias, schema, default_tables)
        # #########print("COL ID: ",col_id)
        ##############print("REPLACE COL: ",replace_col)
        if agg_id !=AGG_OPS.index("none"):
            assert idx < len_ and toks[idx] == ')'
            idx += 1
        if replace_col:
            return idx, col_id,True
        
        return idx, (agg_id, col_id, isDistinct),False

    if toks[idx] == "distinct":
        idx += 1
        isDistinct = True
    idx, col_id, replace_col= parse_col(toks, idx, tables_with_alias, schema, default_tables)

    if isBlock:
        assert toks[idx] == ')'
        idx += 1  # skip ')'
    ##############print("TT", tables_with_alias)
    if replace_col:
            return idx, col_id,True
    return idx, (agg_id, col_id, isDistinct),False


def parse_val_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    ###print(toks[idx])
    if toks[idx] == '(':
        isBlock = True
        idx += 1
    # ##########print("isblock", isBlock)
    col_unit1 = None
    col_unit2 = None
    unit_op = UNIT_OPS.index('none')


    idx, col_unit1,replace_or_not = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
    if idx < len_ and toks[idx] in UNIT_OPS:
        unit_op = UNIT_OPS.index(toks[idx])
        idx += 1
        idx, col_unit2,replace_or_not = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
    if isBlock:
        ###print( toks[idx])
        assert toks[idx] == ')'
        idx += 1  # skip ')'
    if replace_or_not:
        return idx, col_unit1,True
    return idx, (unit_op, col_unit1, col_unit2),False


def parse_table_unit(toks, start_idx, tables_with_alias, schema):
    """
        :returns next idx, table id, table name
    """
    idx = start_idx
    len_ = len(toks)
    # ###print(tables_with_alias)
    # #######print("TABLE", toks[idx])
    key = tables_with_alias[toks[idx]]
    # ##########print(key)
    # ##########print("RR: ", toks[idx])

    # ##########print("KEY: ",key)
    if idx+1<len_ and toks[idx+1] in tables_with_alias:
        idx+=1
    if idx + 1 < len_ and toks[idx+1] == "as":

        idx += 3
    else:
        idx += 1

    return idx, schema.idMap[key], key


def parse_value(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    # ##########print("parse_value",toks[idx])
    len_ = len(toks)
    ###print("toks in val unit", toks[idx])
    #####print(toks[idx])
    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    if toks[idx] == 'select':
        #######print("+++++++++++++++++++++YES")
        idx, val = parse_sql(toks, idx, tables_with_alias, schema, default_tables)
        #######print("&&&&&&&&&&&&&&&&&&")
        #######print("dj",idx)
        #######print("_____________________")
        #######print("BIO",toks[idx])
        #######print(val)
    elif "\"" in toks[idx] or toks[idx] == "''":  # token is a string value
        val = toks[idx]
        idx += 1
    else:
        try:
            val = float(toks[idx])
            idx += 1
        except:
            ###print("HERE")
            end_idx = idx

            while end_idx < len_ and toks[end_idx] != ',' and toks[end_idx] != ')'\
                and toks[end_idx]not in COND_OPS and toks[end_idx] not in CLAUSE_KEYWORDS and toks[end_idx] not in JOIN_KEYWORDS:
                    end_idx += 1
            ###print(toks[start_idx: end_idx])
            idx, val,_ = parse_col_unit(toks[start_idx: end_idx], 0, tables_with_alias, schema, default_tables)
            idx = end_idx
            # ##########print("toks[end]",toks[end_idx])

    if isBlock:
        assert toks[idx] == ')'
        idx += 1

    return idx, val


def parse_condition(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)
    conds = []
    ##print("ALL:", toks[idx])

    while idx < len_:
        ##print("tok:",toks[idx])
        # ##########print(conds)
        if toks[idx] == '(':
            ##print("HIII")
            if toks[idx+1] != "select":
                idx += 1  # Skip '('
                idx, nested_conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
                ##print("_______________________")
                ##print(toks[idx])
                ##print(nested_conds)
                ##print(len(nested_conds))
                if len(nested_conds)==1:
                    conds.append(nested_conds[0])
                else:

                    conds= conds+nested_conds # Add the parsed group of conditions
                    ##print(conds)
                    ##print(len(conds))
                    if len(conds)==1:
                        conds = conds[0]
                ##print(conds)

                assert toks[idx] == ')', "Expected ')' to close the condition group"
                idx += 1  # Skip ')'
                ##print("inner-tok:",toks[idx])

                if idx < len_ and toks[idx] in COND_OPS:
                    conds.append(toks[idx])
                    # ##########print(conds)

                    idx += 1  # skip and/or
                    ##print("YES")
                    continue
                if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
                    break
                if idx>=len_:
                    break

            
            #############print(conds)
        
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            if toks[idx+1]=="(":
                agg_id = AGG_OPS.index(toks[idx])
                idx += 1
            
        if toks[idx] == 'not' and toks[idx+1]== "exists":

            # #############print(toks[idx+1])
            op_id =WHERE_OPS.index(toks[idx+1])
            # #############print("HIII")
            idx += 2  # Skip 'not_exists'
            assert toks[idx] == '(', "Expected '(' after NOT EXISTS"
            # idx += 1  # Skip '('
            # #############print("___",toks[idx])
            idx, subquery_sql = parse_sql(toks, idx, tables_with_alias, schema, default_tables)
            not_op= True
            conds.append((not_op, op_id,None, subquery_sql,None))
            # assert toks[idx] == ')', "Expected ')' to close subquery"
            # idx += 1  # Skip ')'
            # #############print("DENE")

            if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
                break

            if idx < len_ and toks[idx] in COND_OPS:
                conds.append(toks[idx])
                idx += 1  # skip and/or
                continue
            if idx>=len_:
                break


        
        if toks[idx]== "exists":

            op_id =WHERE_OPS.index(toks[idx])
            # #############print("HIII")
            idx += 1
            assert toks[idx] == '(', "Expected '(' after NOT EXISTS"
            # idx += 1  # Skip '('
            # #############print("___",toks[idx])
            idx, subquery_sql = parse_sql(toks, idx, tables_with_alias, schema, default_tables)
            not_op= False
            conds.append((not_op, op_id,None, subquery_sql,None))
            # assert toks[idx] == ')', "Expected ')' to close subquery"
            # idx += 1  # Skip ')'
            # #############print("DENE")
            ############print("++++",toks[idx])

        
            if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
                break

            if idx < len_ and toks[idx] in COND_OPS:
                conds.append(toks[idx])
                idx += 1  # skip and/or
                continue
            if idx>=len_:
                break

        if toks[idx]=="(" and  toks[idx+1]=="select":
            idx, subquery_sql = parse_sql(toks, idx, tables_with_alias, schema, default_tables)
            # ########print(subquery_sql)
            val_unit = subquery_sql
            replace_or_not= False
            # assert toks[idx]==")"
            # idx+=1
        else:
            try:
                idx, val_unit,replace_or_not = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
            except:
                idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                replace_or_not =False


        
        ##print("VAL UNIT: ",val_unit)
        # ########print(toks[idx])
        not_op = False
        if toks[idx] == 'not':
            not_op = True
            idx += 1

        assert idx < len_ and toks[idx] in WHERE_OPS, "Error condition: idx: {}, tok: {}".format(idx, toks[idx])
        op_id = WHERE_OPS.index(toks[idx])
        idx += 1
        val1 = val2 = None
        
        if op_id == WHERE_OPS.index('between'):  # between..and... special case: dual values
            idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
            # ########print(val1)
            assert toks[idx] == 'and'
            idx += 1
            idx, val2 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
            # ########print(val2)
        elif op_id == WHERE_OPS.index('is'):
            # ##########print("OOOOO",toks[idx])
            
            # Handle IS NULL / IS NOT NULL
            op_id = WHERE_OPS.index('is')
            not_op = False
            if toks[idx] == 'null':
                # ##########print("HHH")
                val1 = None  # Represent NULL
                val2 = None
                # conds.append((not_op, op_id, None, val1, val2))
                idx += 1  # Skip 'NULL'
            elif toks[idx] == 'not' and toks[idx + 1] == 'null':
                # ##########print("jjj")
                not_op = True
                val1 = None  # Represent NULL
                val2 = None
                # conds.append((not_op, op_id, None, val1, val2))
                idx += 2  # Skip 'NOT NULL'
            else:
                raise SyntaxError("Invalid syntax after IS")
        elif op_id == WHERE_OPS.index('in'):
            # Handle IN (..)
            assert toks[idx] == '(', "Expected '(' after IN"
            idx += 1  # Skip '('

            if toks[idx] == 'select':
                # Subquery in IN clause
                idx, subquery_sql = parse_sql(toks, idx, tables_with_alias, schema, default_tables)
                val1 = subquery_sql
                val2 =None
                # conds.append((not_op, op_id, val_unit, subquery_sql, None))
            else:
                # List of values in IN clause
                values = []
                while idx < len_ and toks[idx] != ')':
                    idx, value = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                    values.append(value)
                    if toks[idx] == ',':
                        idx += 1  # Skip ','
                    val1 = values
                # conds.append((not_op, op_id, val_unit, values, None))


            assert toks[idx] == ')', "Expected ')' to close IN clause"
            idx += 1  # Skip ')'
            ############print(toks[idx])
            # if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
            #     break

            # if idx < len_ and toks[idx] in COND_OPS:
            #     conds.append(toks[idx])
            #     idx += 1  # skip and/or
            # if idx>=len_:
            #     break
        else:  # normal case: single value
            try:
                ##print("uu",toks[idx])

                idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                #######print("HH",toks[idx])
                ##print("val1: ",val1)
            except:
                ##############print(tables_with_alias)

                idx, val1,_ = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
                # ##########print("except, val1:",val1)


            val2 = None
        # ##print("tttoook", toks[idx])
        if replace_or_not:
            conds.append((not_op, op_id, val_unit, val1, val2))
        else:
            ##print("KLK")
            ##print(val_unit)
            ##print((agg_id,val_unit[1],None))

            conds.append((not_op, op_id, (agg_id,val_unit[1],None), val1, val2))
            ##print(conds)

        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
            break

        if idx < len_ and toks[idx] in COND_OPS:
            # ########print(toks[idx])
            # ########print(toks[idx+1])
            # ########print("fjkdk")
            conds.append(toks[idx])
            # ##########print("AND: cond", conds)
            idx += 1  # skip and/or
            ############print("rr",toks[idx])
    ##print("$$$CONDS",conds)
    return idx, conds


def parse_select(toks, start_idx, tables_with_alias, schema, default_tables=None):
    idx = start_idx
    len_ = len(toks)

    assert toks[idx] == 'select', "'select' not found"
    idx += 1
    isDistinct = False
    if idx < len_ and toks[idx] == 'distinct':
        idx += 1
        isDistinct = True
    val_units = []

    while idx < len_ and toks[idx] not in CLAUSE_KEYWORDS:
        ###print("select",toks[idx])
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
        try:
            idx, val_unit,_ = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        except:
            idx, val_unit = parse_value(toks, idx, tables_with_alias, schema, default_tables)
        # Check for alias usage with "AS"
        ##print("IN SELECT: ",val_unit)
        if idx < len_ and toks[idx].lower() == 'as':
            idx += 1  # Skip "AS
            ##print("gggg")
            ##print(toks[idx])
            ##print(val_unit)
            tables_with_alias[toks[idx]] = val_unit
            if idx < len_:
                # alias = toks[idx]
                # tables_with_alias[alias] = (agg_id, val_unit)  # Map alias to expression
                idx += 1  # Move past the alias token
        else:
            # Handle implicit alias if it follows immediately after the value unit
            if idx < len_ and toks[idx] not in CLAUSE_KEYWORDS and toks[idx] != ',':
                # alias = toks[idx]
                # tables_with_alias[alias] = (agg_id, val_unit)
                idx += 1  # Move past the alias token

        val_units.append((agg_id, val_unit))
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','

    return idx, (isDistinct, val_units)




def parse_from(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    Assume in the from clause, all table units are combined with join
    """
    assert 'from' in toks[start_idx:], "'from' not found"

    len_ = len(toks)
    idx = toks.index('from', start_idx) + 1
    if default_tables== None:
        default_tables=[]

    table_units = []
    conds = []
    join_types = []
    is_subquery = False
    alias_counter=0
    while idx < len_:
        isBlock = False
        #############print("^^Toks[idx]", toks[idx])
        if toks[idx] == '(':
            isBlock = True
            idx += 1

        if toks[idx] == 'select':
            ####print("HIIIII")
          

            idx, sql = parse_sql(toks, idx, tables_with_alias, schema,default_tables)
            #######print(")))))))))))))))")
            #######print(idx)
            #######print(toks[idx])
            #############print(tables_with_alias)
            #############print(twa)
            # del tables_with_alias
            # tables_with_alias = copy.deepcopy(twa)
            #############print(tables_with_alias)
            is_subquery =True
            assert toks[idx] == ')'
            idx+=1
            if  idx<len_ and toks[idx]== "as":
                idx+=1
            ###########print(idx)
            #######print("NOWWW",toks[idx])
            #############print("SQL", sql)
            #############print(toks[idx])
            if  idx<len_ and toks[idx] not in KEYWORDS and toks[idx]!=")":
                subquery_alias = toks[idx]
                idx+=1

            else:
                subquery_alias = f"subquery_{alias_counter}"
                alias_counter += 1
            #############print("SUBQUERY_ALIAS", subquery_alias)
            subquery_columns = [col[1][1][1] for col in sql['select'][1]]
            #############print(subquery_columns)
            tables_with_alias[subquery_alias] = {
                    "type": "subquery",
                    "columns": subquery_columns,
                    "sql": sql
                }  
            ###########print("&&&&&&&&&&&&&&&&",tables_with_alias)              
            table_units.append((TABLE_TYPE['sql'], sql))
            # ########print(table_units)
        else:
            ###########print("++++",toks[idx])
            if idx< len_ and toks[idx] in JOIN_TYPES:

                join_types.append( JOIN_TYPES.index(toks[idx]))
                idx+=1
            if idx < len_ and toks[idx] == 'join':
                idx += 1  # skip join
            idx, table_unit, table_name = parse_table_unit(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE['table_unit'],table_unit))
            default_tables.append(table_name)
        if idx < len_ and toks[idx] == "on":
            idx += 1  # skip on
            ###########print("JII")
            idx, this_conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
            ###########print("HHH")
            if len(conds) > 0:
                conds.append('and')
            conds.extend(this_conds)
        # #############print(table_units)

        if isBlock and not is_subquery:

            assert toks[idx] == ')'

            idx += 1
            #############print(idx)
            #############print(len_)
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
            break
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip comma and continue parsing the next table
            continue
    
    #############print("DONE@@")
    # tables_with_alias = copy.deepcopy(twa)
    #############print(tables_with_alias)

    return idx, table_units, conds, default_tables, join_types


def parse_where(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'where':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_group_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    col_units = []

    if idx >= len_ or toks[idx] != 'group':
        return idx, col_units

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, col_unit,_ = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
        col_units.append(col_unit)
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, col_units


def parse_order_by(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)
    val_units = []
    order_type = 'asc' # default type is 'asc'

    if idx >= len_ or toks[idx] != 'order':
        return idx, val_units

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            if toks[idx+1]=="(":
                agg_id = AGG_OPS.index(toks[idx])
                idx += 1
        if toks[idx]=="null":
            val_units.append("null")  # Add 'null' as a marker for no sorting
            idx += 1
            break  # No need to parse

        ##print(toks[idx])
        idx, val_unit, _ = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        ##print("ORDERBY val",val_unit)
        ##print(_)
        ##print(val_unit[1])
        ##print("++++")
        if not _:
            ##print("KK")
            val_units.append((agg_id,val_unit[1],None))
        else:
            val_units.append((agg_id,val_unit[1],None))
        ##########print(val_unit)
        if idx < len_ and toks[idx] in ORDER_OPS:
            order_type = toks[idx]
            idx += 1
        if idx < len_ and toks[idx] == ',':
            idx += 1  # skip ','
        else:
            break

    return idx, (order_type, val_units)


def parse_having(toks, start_idx, tables_with_alias, schema, default_tables):
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'having':
        return idx, []   

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_limit(toks, start_idx):
    idx = start_idx
    len_ = len(toks)

    if idx < len_ and toks[idx] == 'limit':
        idx += 2
        # #print(toks[idx])
        # make limit value can work, cannot assume put 1 as a fake limit number
        try :
            offset_val = float(toks[idx-1])
            return idx, offset_val
        except:
             return idx, 10000


    return idx, None
def parse_offset(toks, start_idx):
    idx = start_idx
    len_ = len(toks)
    if idx < len_ and toks[idx] == 'offset':
        idx += 2
        # #print(toks[idx-1])
        # make limit value can work, cannot assume put 1 as a fake limit number
        try :
            offset_val = float(toks[idx])
            return idx, offset_val
        except:
             return idx, 10000
    else:
        return idx, None





def parse_sql(toks, start_idx, tables_with_alias, schema, default_tables=None):
    isBlock = False # indicate whether this is a block of sql/sub-sql
    len_ = len(toks)
    idx = start_idx

    sql = {}
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # parse from clause in order to get default tables
    from_end_idx, table_units, conds, default_tables , join_types= parse_from(toks, start_idx, tables_with_alias, schema, default_tables)
    ###print("FROM INFO_________________________________\n",from_end_idx, table_units, conds, default_tables)
    
    sql['from'] = {'table_units': table_units, 'conds': conds, 'join_types':join_types}
    # select clause
    ############print("MMMMMMMAN1",tables_with_alias)
    _, select_col_units = parse_select(toks, idx, tables_with_alias, schema, default_tables)
    #############print("MMMMMMMAN2",tables_with_alias)

    ###print("##SELECT: ",select_col_units)
    idx = from_end_idx
    sql['select'] = select_col_units
    
    # where clause
    idx, where_conds = parse_where(toks, idx, tables_with_alias, schema, default_tables)
    ##print("##Where: ",where_conds)
    #############print("MMMMMMMAN3",tables_with_alias)
    sql['where'] = where_conds
    # group by clause
    idx, group_col_units = parse_group_by(toks, idx, tables_with_alias, schema, default_tables)
    ########print("##Groupby: ",group_col_units)
    #############print("MMMMMMMAN4",tables_with_alias)

    sql['groupBy'] = group_col_units
    # having clause
    idx, having_conds = parse_having(toks, idx, tables_with_alias, schema, default_tables)
    #######print("##having: ",having_conds)
    #############print("MMMMMMMAN5",tables_with_alias)

    sql['having'] = having_conds
    # order by clause
    idx, order_col_units = parse_order_by(toks, idx, tables_with_alias, schema, default_tables)
    # ########print(order_col_units)
    #############print("MMMMMMMAN6",tables_with_alias)

    sql['orderBy'] = order_col_units
    # limit clause
    idx, limit_val = parse_limit(toks, idx)
    #############print("MMMMMMMAN7",tables_with_alias)

    # #print("limit",limit_val)
    sql['limit'] = limit_val
    idx, offset_val = parse_offset(toks, idx)

    sql['offset'] = offset_val
    

    idx = skip_semicolon(toks, idx)
    #######print("isBALCK", isBlock)
    
    if isBlock:
        assert toks[idx] == ')'
        idx += 1  # skip ')'
    idx = skip_semicolon(toks, idx)

    #############print("DONE")
    # intersect/union/except clause
    # ##########print(toks[idx])
    for op in SQL_OPS:  # initialize IUE
        sql[op] = None
        # ##########print(op)
    if idx < len_ and toks[idx] in SQL_OPS:
        sql_op = toks[idx]
        # ##########print("+++++++++++++++++++++++++++++++++++++++++++",sql_op)
        idx += 1
        idx, IUE_sql = parse_sql(toks, idx, tables_with_alias, schema,default_tables)
        sql[sql_op] = IUE_sql
    #############print("MMMMMMMAN7",tables_with_alias)


    return idx, sql


def load_data(fpath):
    with open(fpath) as f:
        data = json.load(f)
    return data


def get_sql(schema, query):
    toks = tokenize(query)
    # #####print(toks)
    tables_with_alias = get_tables_with_alias(schema.schema, toks)
    # ##print("Tables with alias",tables_with_alias)
    _, sql = parse_sql(toks, 0, tables_with_alias, schema)
    ####print(sql)

    return sql


def skip_semicolon(toks, start_idx):
    idx = start_idx
    while idx < len(toks) and toks[idx] == ";":
        idx += 1
    return idx

def isValidSQL(sql, db):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except:
        return False
    return True
import os
if __name__ == "__main__":
    ################################
# Assumptions:
#   1. sql is correct
#   2. only table name has alias
#   3. only one intersect/union/except
#
# val: number(float)/string(str)/sql(dict)
# col_unit: (agg_id, col_id, isDistinct(bool))
# val_unit: (unit_op, col_unit1, col_unit2)
# table_unit: (table_type, col_unit/sql)
# cond_unit: (not_op, op_id, (agg_id,val_unit), val1, val2)
# condition: [cond_unit1, 'and'/'or', cond_unit2, ...]
# sql {
#   'select': (isDistinct(bool), [(agg_id, val_unit), (agg_id, val_unit), ...])
#   'from': {'table_units': [table_unit1, table_unit2, ...], 'conds': condition}
#   'where': condition
#   'groupBy': [col_unit1, col_unit2, ...]
#   'orderBy': ('asc'/'desc', [(agg_id, val_unit1), (agg_id, val_unit2), ...])
#   'having': condition
#   'limit': None/limit value
#   'intersect': None/sql
#   'except': None/sql
#   'union': None/sql
# }
################################
    sql ='SELECT COUNT(T1.paper_id) FROM Paper AS T1 JOIN Citation AS T2 ON T1.paper_id = T2.paper_id JOIN Paper AS T3 ON T2.cited_paper_id = T3.paper_id WHERE T1.year = 2020 LIMIT 10'

    sql = """SELECT T1.name AS affiliation_name, T2.name AS author_name FROM Affiliation AS T1, Author AS T2, Author_list AS T3 WHERE T1.affiliation_id = T3.affiliation_id AND T2.author_id = T3.author_id"""
    # sql = """SELECT T1.name, COUNT(DISTINCT T2.name) FROM buildings AS T1 JOIN Office_locations AS T3 ON T1.id = T3.building_id JOIN Companies AS T2 ON T2.id = T3.company_id WHERE T1.Height >= 100 ORDER BY T1.name ASC"""
    # sql ="SELECT avg(product_type_code) FROM Products WHERE avg(product_type_code)  =  \"Clothes\""
    # sql = """SELECT T1.product_name, T2.product_name FROM Products AS T1 JOIN Products AS T2 ON T1.product_id = T2.product_id WHERE T1.product_price BETWEEN 10 AND 20 OR T1.product_description IN ('sample1', 'sample2') ORDER BY T1.product_price ASC LIMIT 10"""
    # sql ="""SELECT COUNT(DISTINCT T1.City) FROM city AS T1 WHERE T1.City_ID IN ( SELECT T2.Host_City FROM hosting_city AS T2 WHERE EXISTS ( SELECT 1 FROM match AS T3 WHERE T3.Match_ID = T2.Match_ID ) ) OR AVG(T1.Regional_Population) > 1000000 OR COUNT(T1.GDP) < 10"""
    # sql = """SELECT DISTINCT T1.Festival_Name, T3.Type FROM festival_detail AS T1 JOIN nomination AS T2 ON T1.Festival_ID = T2.Festival_ID JOIN artwork AS T3 ON T2.Artwork_ID = T3.Artwork_ID WHERE (T1.Year BETWEEN 2010 AND 2020) OR T3.Name = 'Painting' OR T1.Location = 'London' LIMIT 10"""
    # sql ="""SELECT T1.Name FROM (SELECT T1.Name, T2.Party_ID FROM host AS T1 JOIN party_host AS T2 ON T1.Host_ID = T2.Host_ID) AS T1 WHERE T1.Party_ID >= 1"""
    # sql = """SELECT T1.product_name FROM Products AS T1 JOIN Order_Items AS T2 ON T1.product_id = T2.product_id JOIN Orders AS T3 ON T3.order_id = T2.order_id WHERE T2.order_item_id IN (SELECT T1.order_item_id FROM Invoice_Line_Items AS T1 JOIN Invoices AS T2 ON T1.invoice_number = T2.invoice_number WHERE T2.invoice_date > '2020-01-01') ORDER BY T1.product_name ASC"""
    # sql ="""SELECT T1.uid, T1.name FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid = T2.f1 WHERE T1.followers < (SELECT COUNT(f2) FROM follows GROUP BY f2 HAVING f2 = T2.f2) ORDER BY T1.uid ASC"""
    # sql = """SELECT T1.customer_name, T2.customer_name FROM Customers AS T1 JOIN Customers AS T2 ON T1.customer_id = T2.customer_id WHERE T1.date_became_customer < T2.date_became_customer GROUP BY T1.customer_name HAVING MIN(T1.date_became_customer) ORDER BY T1.date_became_customer DESC LIMIT 100"""
    # sql = """SELECT COUNT(product_id), COUNT(DISTINCT order_id) FROM Order_Items"""
    # sql = """SELECT T1.name FROM languages AS T1 JOIN official_languages AS T2 ON T1.id = T2.language_id WHERE NOT EXISTS (SELECT T3.name FROM countries AS T3 WHERE T3.id = T2.country_id) GROUP BY T1.name HAVING MAX(T3.overall_score) IS NULL ORDER BY T1.name DESC LIMIT 10"""
    # sql ="""SELECT T1.Title, T2.Title FROM book AS T1, book AS T2 WHERE T1.Type = T2.Type AND T1.Chapters = T2.Chapters OR T1.Pages BETWEEN 100 AND 200 OR T1.Title IN (SELECT Title FROM book)"""
#     sql ="""SELECT T2.School, T1.Team_Name FROM basketball_match AS T1 JOIN university AS T2 ON T1.School_ID = T2.School_ID JOIN basketball_match AS T3 ON T2.School_ID = T3.School_ID WHERE (T1.All_Games_Percent BETWEEN 0.5 AND 0.7) OR (T3.All_Games_Percent > 0.8) ORDER BY T2.School ASC
# """
    # sql = """SELECT T1.order_id, T2.order_id FROM Customer_Orders AS T1 JOIN Customer_Orders AS T2 ON T1.order_id = T2.order_id WHERE T1.order_shipping_charges > T2.order_shipping_charges GROUP BY T1.order_id HAVING MAX(T1.order_shipping_charges) ORDER BY T1.order_id DESC"""
#     sql ="""
# SELECT DISTINCT T1.Title, COUNT(T1.Title) FROM ( SELECT T1.Title FROM Songs AS T1 UNION ALL SELECT T2.Title FROM Albums AS T2 ) AS T1 WHERE T1.Title BETWEEN 'A' AND 'E' OR T1.Title IN ( SELECT T3.Title FROM Albums AS T3 WHERE T3.Year > 2000 ) ORDER BY T1.Title DESC"""
    # sql ="SELECT T1.order_date ,  T1.order_id FROM Customer_Orders AS T1 JOIN Order_items AS T2 ON T1.order_id  =  T2.order_id WHERE T2.order_quantity  >  6 UNION ALL SELECT T1.order_date ,  T1.order_id FROM Customer_Orders AS T1 JOIN Order_items AS T2 ON T1.order_id  =  T2.order_id GROUP BY T1.order_id HAVING count(*)  >  3;"
    # db_id = "customers_and_orders"
    # sql ="""SELECT COUNT(DISTINCT T5.Title), T1.Title FROM Songs AS T1 JOIN Tracklists AS T2 ON T1.SongId = T2.SongId JOIN Albums AS T3 ON T2.AlbumId = T3.AId JOIN Performance AS T4 ON T3.AId = T4.SongId JOIN Songs AS T5 ON T4.SongId = T5.SongId WHERE T3.Type = 'Compilation' OR T3.Label = 'Universal' OR T1.SongId IN (SELECT SongId FROM Instruments WHERE Instrument = 'Guitar') LIMIT 5"""
    # sql = """SELECT train_number, name, origin, destination FROM train AS T1 WHERE EXISTS ( SELECT * FROM route AS T2 WHERE T2.train_id = T1.id ) AND EXISTS ( SELECT * FROM weekly_weather AS T3 WHERE T3.station_id = 123 )"""
    # sql = """SELECT document_type_description FROM Ref_Document_Types WHERE document_type_code = 'D001' AND document_type_code BETWEEN 'D001' AND 'D005' AND Length(document_type_description) > 10 LIMIT 5"""
    # sql ="""SELECT T1.Name FROM musical AS T1 WHERE T1.Year BETWEEN 2000 AND 2010 OR T1.Award = 'Tony Award' OR T1.Result = 'Win', T1.Name ORDER BY T1.Name DESC"""
#     sql = """SELECT T1.Name, T3.Number_of_Championships FROM institution AS T1 JOIN Championship AS T2 ON T1.Institution_ID = T2.Institution_ID JOIN ( SELECT Institution_ID FROM Championship WHERE Number_of_Championships > 1 ) AS T3 ON T1.Institution_ID = T3.Institution_ID ORDER BY T1.Name ASC
# """
    # sql ="""SELECT T1.Model, COUNT(DISTINCT T3.Name) FROM vehicle AS T1 JOIN vehicle_driver AS T2 ON T1.Vehicle_ID = T2.Vehicle_ID JOIN driver AS T3 ON T2.Driver_ID = T3.Driver_ID GROUP BY T1.Model HAVING COUNT(DISTINCT T3.Name) = (SELECT MAX(mycount) FROM (SELECT COUNT(DISTINCT T3.Name) as mycount FROM vehicle AS T1 JOIN vehicle_driver AS T2 ON T1.Vehicle_ID = T2.Vehicle_ID JOIN driver AS T3 ON T2.Driver_ID = T3.Driver_ID GROUP BY T1.Model)) LIMIT 1"""
    # sql= """SELECT T1.name FROM aircraft AS T1 JOIN ( SELECT aid FROM flight WHERE price > 10000 ) AS T2 ON T1.aid = T2.aid ORDER BY T1.name ASC LIMIT 10"""
    # sql = """SELECT T1.CITY_NAME, T2.STREET_NAME FROM RESTAURANT AS T1 INNER JOIN LOCATION AS T2 ON T1.CITY_NAME = T2.CITY_NAME WHERE T1.NAME = 'Joe''s Diner'"""
    sql ="""SELECT DISTINCT T1.Store_Name FROM Drama_Workshop_Groups AS T1 JOIN Stores AS T2 ON T1.Address_ID = T2.Address_ID WHERE T2.Store_Phone != 'kldkl''s kdk' LIMIT 10"""
    sql ="""SELECT * FROM ( SELECT T1.first_name, T2.name FROM customers AS T1 INNER JOIN genres AS T2 ON T1.id = 1 AND T1.first_name = 'Alicia' WHERE T1.id NOT IN ( SELECT T3.customer_id FROM invoices AS T3 WHERE T3.total < 10 ) )"""
    sql = """SELECT T1.name FROM aircraft AS T1 JOIN ( SELECT aid FROM flight WHERE price > 10000 ) AS T2 ON T1.aid = T2.aid ORDER BY T1.name ASC LIMIT 10"""
    sql ="""SELECT MIN(Paragraph_ID) AS Min_Paragraph_ID, MAX(Paragraph_ID) AS Max_Paragraph_ID, Other_Details FROM Paragraphs WHERE (Paragraph_ID BETWEEN 1 AND 10 OR Paragraph_ID > 23) GROUP BY Other_Details HAVING SUM(Paragraph_ID) > 81"""
    sql ="""SELECT DISTINCT T.Template_Type_Code FROM Templates T"""
    sql ="""SELECT Conductor_ID, COUNT(DISTINCT Year_of_Work) AS distinct_years FROM conductor WHERE Conductor_ID < 66 GROUP BY Conductor_ID ORDER BY distinct_years DESC LIMIT 8"""
    sql ="""SELECT o.Orchestra, p.Date, o.Major_Record_Format FROM orchestra o JOIN performance p ON o.Orchestra_ID = p.Orchestra_ID ORDER BY o.Orchestra, p.Date"""
    sql ="""SELECT 1 FROM Courses WHERE course_id = 68 LIMIT 1"""
    sql = """SELECT c.Population, c.GNP FROM country c LEFT JOIN countrylanguage cl ON c.Code = cl.CountryCode WHERE c.IndepYear != 21 OR (cl.IsOfficial = 'T' AND cl.Percentage BETWEEN 1 AND 10) GROUP BY c.Code	
"""
    sql ="""SELECT T1.Course FROM course AS T1 JOIN course_arrange AS T2 ON T1.Course_ID = T2.Course_ID WHERE T2.Teacher_ID != 1"""
    sql ="""SELECT Template_Type_Description FROM Ref_Template_Types WHERE Template_Type_Code BETWEEN 'A' AND 'Z' ORDER BY Template_Type_Code DESC LIMIT 10 OFFSET 10"""
    sql = """SELECT Orchestra FROM orchestra ORDER BY Orchestra ASC LIMIT 1 OFFSET 1;"""
    db_id = "orchestra"

    # db_id = "allergy_1"
    # sql =  "SELECT avg(age) FROM Student WHERE StuID IN ( SELECT T1.StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"food\" INTERSECT SELECT T1.StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"animal\")"

    table_file = "sql_generator/data/tables.json"
    all_db = convert_json_to_schema("data/tables.json", col_exp=False)
    #print(all_db[db_id])

    db = db_id
    ##############print(db)
    # db = "test-suite-sql-eval-master/database/twitter_1/twitter_1.sqlite"
    ##############print(db)
    # # schema = Schema(get_schema(db))
    # ##############print(schema.schema)
    # ##############print("_________")
    # ##############print(sql)
    # # g_sql = get_sql(schema, sql)
    # ##############print(g_sql)
    # evaluator = Evaluator()

    db2 = os.path.join("test-suite-sql-eval-master/database/", db_id, db_id + ".sqlite")
    ########print(isValidSQL(sql,db2))
    ##############print(db2)
    schema2 = Schema(get_schema(db2))

    query = sql
    ##############print("query:",query)
    
    g_sql = get_sql(schema2, query=query)
    #######print("_________________")
    #print("Parsed SQL:", g_sql)
    #print(g_sql["where"])
    ############print("_________________")
    count_table = len(g_sql["from"]["table_units"])
    # #############print(count_table)
    ##########print(g_sql["from"]["table_units"])

    # ##############print(get_nestedSQL(g_sql)) 
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'test-suite-sql-eval-master')))
    tsa = importlib.import_module("evaluation") 
  
    # hardness = evaluator.eval_hardness(g_sql)
    # ##print(tsa.count_component2(g_sql))
    #####print(hardness)
        