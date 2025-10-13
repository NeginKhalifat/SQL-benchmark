import os
import random
from openai import OpenAI
from json_repair import repair_json
def make_request(messages,client, model_name):
    response = client.chat.completions.create(
        model=model_name,
        messages=messages,
        max_tokens=512,
 
    )
    good_json_string = repair_json(response.choices[0].message.content,return_objects=True)
  

    return good_json_string
def query_generator_single_schema_llm(client,model_name, db_name, schema, pk, fk, schema_types,all_db, specs, max_num=20, write_to_csv=True, random_choice=True):        
  
    CONSTRUCT_SQL_MSGS =[
    {
        "role": "system",
 "content": f"""You are an SQL query generator.Generate an SQLite query based on the provided specifications. Follow these guidelines:
Specification Details: Adhere closely to schema details, including primary keys (PK), foreign keys (FK), column types, and data.
Data Usage: Utilize the provided table data to construct a query that returns at least one row.
Query Construction:
Ensure the query is logical and meaningful.
Do not use placeholders (e.g., question marks). Instead, use appropriate random values according to the data type.
Output Format: Return the query in JSON format with the key 'query'."""   },
    {
        "role": "user",
        "content": f"""{{"spec": {{"meaningful_joins": "yes", "table_exp_type": "single_table", "where_type": "none", "number_of_value_exp_in_group_by": 0, "having_type": "none", "orderby_type": "DESC", "limit_type": "none", "value_exp_types": "*", "distinct_type": "none", "min_max_depth_in_subquery": [1, 1]}}, "schema": {all_db["ship_1"]["schema"]}, "pk": {all_db["ship_1"]["primary_keys"]}, "fk": {all_db["ship_1"]["foreign_keys"]}, "column_types": {all_db["ship_1"]["schema_types"]},"data":{{'Ship': [(1, 'HMS Manxman', 'Panamax', 1997.0, 'KR', 'Panama'),
  (2, 'HMS Gorgon', 'Panamax', 1998.0, 'KR', 'Panama'),
  (3, 'HM Cutter Avenger', 'Panamax', 1997.0, 'KR', 'Panama'),
  (4, 'HM Schooner Hotspur', 'Panamax', 1998.0, 'KR', 'Panama'),
  (5, 'HMS Destiny', 'Panamax', 1998.0, 'KR', 'Panama'),
  (6, 'HMS Trojan', 'Panamax', 1997.0, 'KR', 'Panama'),
  (7, 'HM Sloop Sparrow', 'Panamax', 1997.0, 'KR', 'Panama'),
  (8, 'HMS Phalarope', 'Panamax', 1997.0, 'KR', 'Panama'),
  (9, 'HMS Undine', 'Panamax', 1998.0, 'GL', 'Malta')],
 'captain': [(1,
   'Captain Sir Henry Langford',
   1,
   '40',
   'Third-rate ship of the line',
   'Midshipman'),
  (2,
   'Captain Beves Conway',
   2,
   '54',
   'Third-rate ship of the line',
   'Midshipman'),
  (3, 'Lieutenant Hugh Bolitho', 3, '43', 'Cutter', 'Midshipman'),
  (4, 'Lieutenant Montagu Verling', 4, '45', 'Armed schooner', 'Midshipman'),
  (5, 'Captain Henry Dumaresq', 5, '38', 'Frigate', 'Lieutenant'),
  (6,
   'Captain Gilbert Pears',
   2,
   '60',
   'Third-rate ship of the line',
   'Lieutenant'),
  (7,
   'Commander Richard Bolitho',
   3,
   '38',
   'Sloop-of-war',
   'Commander, junior captain')]}}}}""",
    },{
        "role": "assistant",
        "content": "{\"query\": \"SELECT *  FROM Ship ORDER BY Name DESC\"}"
    },
    
]

    print("Start reading specifications")
    testing_with_one_spec = False
    if specs is not None:
        testing_with_one_spec = True
    if not testing_with_one_spec:
        print("Testing with multiple specifications")
        file_name =  f"""/home/khalifat/home/khalifat/data/sql_generator/sql-generator/generate_queries/outputs/specs/{db_name}.json"""
        with open(file_name) as json_file:
            specs = json.load(json_file)
    else:
        print("Testing with one specification")
        print(specs)

    print("Start generating queries")
    merged_queries = {}
    return_select_fields_dict = {}
    schema_copy = schema.copy()
    pk_copy = pk.copy()
    fk_copy = fk.copy()
    schema_types_copy = schema_types.copy()
    not_parsed_queries = {}
    sqlite_file = "/home/khalifat/home/khalifat/data/sql_generator/sql-generator/test-suite-sql-eval-master/database/"+ db_name+ "/"+db_name+".sqlite"
    data = get_data(sqlite_file)
    db = db_name
    db = os.path.join("/home/khalifat/home/khalifat/data/sql_generator/sql-generator/test-suite-sql-eval-master/database/", db, db + ".sqlite")
    schema2 = Schema(get_schema(db))
    print(schema.schema)
        
    
    print("HI")
    target_counts = {"easy": 7, "medium": 13, "hard": 5, "extra": 5}
    current_counts = {"easy": 0, "medium": 0, "hard": 0, "extra": 0}

    queries = {"easy": [], "medium": [], "hard": [], "extra": []}
    schema = schema_copy.copy()
    pk = pk_copy.copy()
    fk = fk_copy.copy()
    schema_types = schema_types_copy.copy()

    for i, hash in enumerate(specs[db_name]):
        print("++++++++++")
        print(current_counts)
        if all(current_counts[diff] >= target_counts[diff] for diff in target_counts):
            break  # Break if we have reached the target counts for all difficulty levels

            
        # Need to have a copy of the schema, pk, fk, schema_types because they are modified in the create_table_expression function
       
        print("i", i)
        spec = specs[db_name][hash]

        print("spec",spec)
        

        CONSTRUCT_SQL_MSGS.append(
            { "role": "user",
            "content": f"""{{"spec":{specs[db_name][hash]}, "schema": {schema},"pk": {pk} ,"fk": {fk}, "column_types": {schema_types},"data":{data}}}"""    }
        )
        res = make_request(CONSTRUCT_SQL_MSGS,client, model_name)
        print(res["query"])
        partial_query = res["query"]
        flag = False
        CONSTRUCT_SQL_MSGS.pop()
        
        print("*********")
        print(partial_query)
        try:
            g_sql = get_sql(schema2, partial_query)
            print(g_sql)
            hardness = eval.eval_hardness(g_sql)
            print(hardness)

            if current_counts[hardness] < target_counts[hardness]:
                queries[hardness].append(partial_query)
                current_counts[hardness] += 1
                if str(spec) in merged_queries :
                    merged_queries[str(spec)] += (
                        "\n\n" + partial_query
                    )
                elif (
                    str(spec) not in merged_queries 
                ):
                    merged_queries[str(spec)] = (
                        partial_query
                    )
        except:
            print("Error in parsing")
            continue
            
        
    ADD_SET_OPS_QUERY = [
    {
        "role": "system",
        "content": """You are an SQL query generator. Generate 2 random SQLite queries that contain set operations like UNION, INTERSECT, or EXCEPT. Follow these guidelines:
        
        Specification Details: Adhere closely to schema details, including primary keys (PK), foreign keys (FK), column types, and data.
        
        Data Usage: Utilize the provided table data to construct queries that return at least one row.
        
        Query Construction:
        Ensure the queries are logical and meaningful.
        Do not use placeholders (e.g., question marks). Instead, use appropriate random values according to the data type.
        
        Output Format: Return the queries in JSON format with the keys 'query1' and 'query2'."""
    },
    {
        "role": "user",
        "content": """{
            "schema": {all_db["ship_1"]["schema"]},
            "pk": {all_db["ship_1"]["primary_keys"]},
            "fk": {all_db["ship_1"]["foreign_keys"]},
            "column_types": {all_db["ship_1"]["schema_types"]},
            "data": {
                'Ship': [
                    (1, 'HMS Manxman', 'Panamax', 1997.0, 'KR', 'Panama'),
                    (2, 'HMS Gorgon', 'Panamax', 1998.0, 'KR', 'Panama'),
                    (3, 'HM Cutter Avenger', 'Panamax', 1997.0, 'KR', 'Panama'),
                    (4, 'HM Schooner Hotspur', 'Panamax', 1998.0, 'KR', 'Panama'),
                    (5, 'HMS Destiny', 'Panamax', 1998.0, 'KR', 'Panama'),
                    (6, 'HMS Trojan', 'Panamax', 1997.0, 'KR', 'Panama'),
                    (7, 'HM Sloop Sparrow', 'Panamax', 1997.0, 'KR', 'Panama'),
                    (8, 'HMS Phalarope', 'Panamax', 1997.0, 'KR', 'Panama'),
                    (9, 'HMS Undine', 'Panamax', 1998.0, 'GL', 'Malta')
                ],
                'captain': [
                    (1, 'Captain Sir Henry Langford', 1, '40', 'Third-rate ship of the line', 'Midshipman'),
                    (2, 'Captain Beves Conway', 2, '54', 'Third-rate ship of the line', 'Midshipman'),
                    (3, 'Lieutenant Hugh Bolitho', 3, '43', 'Cutter', 'Midshipman'),
                    (4, 'Lieutenant Montagu Verling', 4, '45', 'Armed schooner', 'Midshipman'),
                    (5, 'Captain Henry Dumaresq', 5, '38', 'Frigate', 'Lieutenant'),
                    (6, 'Captain Gilbert Pears', 2, '60', 'Third-rate ship of the line', 'Lieutenant'),
                    (7, 'Commander Richard Bolitho', 3, '38', 'Sloop-of-war', 'Commander, junior captain')
                ]
            }
        }"""
    },
    {
        "role": "assistant",
        "content": "{\"query1\": \"SELECT * FROM Ship WHERE year_built > 1997 UNION SELECT * FROM captain WHERE age > 40\", \"query2\": \"SELECT name FROM Ship INTERSECT SELECT name FROM captain\"}"
    }
]
    ADD_SET_OPS_QUERY.append(
            { "role": "user",
            "content": f"""{{ "schema": {schema},"pk": {pk} ,"fk": {fk}, "column_types": {schema_types},"data":{data}}}"""    }
        )
    print(ADD_SET_OPS_QUERY)
    res = make_request(ADD_SET_OPS_QUERY,client, model_name)
    print(res["query1"])

    print(res["query2"])
    query1 = res["query1"]
    query2 = res["query2"]

    try:
        g_sql = get_sql(schema2, query1)
        print(g_sql)
        hardness = eval.eval_hardness(g_sql)
        print(hardness)
        spec = "set-op"
        merged_queries["set-op1"] =  query1
        queries[hardness].append(query1)
        current_counts[hardness] += 1
                
           
    except:
        print("Error in parsing")
        
    try:
        g_sql = get_sql(schema2, query2)
        print(g_sql)
        hardness = eval.eval_hardness(g_sql)
        print(hardness)
       
        merged_queries["set-op2"] =  query2
        queries[hardness].append(query2)
        current_counts[hardness] += 1
           
    except:
        print("Error in parsing")
        


    if write_to_csv:
        csv_file = f"/home/khalifat/home/khalifat/data/llama3_70/data/synthetic_queries/llm/{db_name}_res.csv"
        write_queries_to_file(
            merged_queries=merged_queries, db_name=db_name, file_name=csv_file
        )
    with open("/home/khalifat/home/khalifat/data/llama3_70/data/synthetic_queries/llm/stats/queries_and_counts.txt", "w") as file:
        file.write(f"queries = {queries}\n")
        file.write(f"current_counts = {current_counts}\n")
        
       
    print("Done generating queries")
    

        
            
            

def generate_query_using_llm(max_num, random_choice, config_name, n_dbs,db_name=None, specs=None, write_to_csv=False ):
        
        all_db = convert_json_to_schema("/home/khalifat/home/khalifat/data/llama3_70/data/tables.json", col_exp=False)

        config_file = "/home/khalifat/home/khalifat/data/sql_generator/sql-generator/generate_queries/config_file.json"
        client = OpenAI(
        api_key="token-wdmuofa",
        base_url="http://anagram.cs.ualberta.ca:2000/v1" # Choose one from the table
        # base_url = "http://turin4.cs.ualberta.ca:2001/v1"
        
    )
     
        model_name = "meta-llama/Meta-Llama-3-70B-Instruct" # Choose one fro
    
        db_file = "/home/khalifat/home/khalifat/data/llama3_70/data/tables.json"

        if db_name is None:
            # randomly select:
            random.seed(10)

            random_dbs = random.sample(list(all_db.keys()), n_dbs)
            # call query_generator_single_schema for all databases
            for db in random_dbs:
                schema, pk, fk, schema_types = read_schema_pk_fk_types(db, db_file, all_db=all_db, col_exp=False)
                complete_specs(db_file, config_file, db_name=db, num_query=2*max_num)
                try:
                  
                    query_generator_single_schema_llm(client,model_name,db, schema, pk, fk, schema_types,all_db, specs=specs, max_num=max_num, write_to_csv=True, random_choice=random_choice)
                except Exception as e:
                    print(e)
        else:
            # call query_generator_single_schema for the given database
            schema, pk, fk, schema_types = read_schema_pk_fk_types(db_name, db_file, all_db=all_db)
            complete_specs(db_file, config_file, db_name=db_name, num_query=2*max_num)
            try:
                query_generator_single_schema_llm(client,model_name,db_name, schema, pk, fk, schema_types,all_db, specs=specs, max_num=max_num, write_to_csv=True, random_choice=random_choice)
            except Exception as e:
                print(e)

        


if __name__ == "__main__":
    specs = {
        "movie_1": {
            "ae99efea2cbadfa5e336d8fd2a4fd91b0911f8b8": {
                #             "set_op_type": "none",
                #             "first_query": {
                #                 #     "meaningful_joins": "no",
                #                 #     "table_exp_type": "CTE",
                #                 #     "where_type": {
                #                 #         "logical_operator": [
                #                 #             "OR",
                #                 #             "basic_comparison",
                #                 #             "basic_comparison",
                #                 #         ] 
                #                 #     },
                #                 #     "number_of_value_exp_in_group_by": 1,
                #                 #     "having_type": "none",
                #                 #     "orderby_type": "none",
                #                 #     "limit_type": "without_offset",
                #                 #     "value_exp_types": ["agg_exp_alias", "single_exp_text"],
                #                 #     "distinct_type": "none",
                #                 #     "min_max_depth_in_subquery": [1, 1],
                #                 # },
                #                 #     "meaningful_joins": "no",
                #                 #     "table_exp_type": "FULL OUTER JOIN_FULL OUTER JOIN",
                #                 #     "where_type": {
                #                 #         "logical_operator": [
                #                 #             "OR",
                #                 #             "comparison_with_subquery",
                #                 #             "exists_subquery",
                #                 #         ]
                #                 #     },
                #                 #     "number_of_value_exp_in_group_by": 0,
                #                 #     "having_type": "none",
                #                 #     "orderby_type": "multiple",
                #                 #     "limit_type": "none",
                #                 #     "value_exp_types": ["string_func_exp_alias", "subquery_exp_alias"],
                #                 #     "distinct_type": "distinct",
                #                 #     "min_max_depth_in_subquery": [1, 1],
                #                 # },
                #                 #         "meaningful_joins": "yes",
                #                 #         "table_exp_type": "LEFT JOIN_SELF JOIN",
                #                 #         "where_type": "basic_comparison",
                #                 #         "number_of_value_exp_in_group_by": 1,
                #                 #         "having_type": {"single": "COUNT"},
                #                 #         "orderby_type": "number_ASC",
                #                 #         "limit_type": "with_offset",
                #                 #         "value_exp_types": ["string_func_exp_alias"],
                #                 #         "distinct_type": "distinct",
                #                 #         "min_max_depth_in_subquery": [0, 0],
                #                 #     },
                #                 # }
'meaningful_joins': 'yes', 'table_exp_type': 'single_table_with_name_changing', 'where_type': {'logical_operator': ['OR', 'between', 'not_exists_subquery']}, 'number_of_value_exp_in_group_by': 0, 'having_type': 'none', 'orderby_type': 'none', 'limit_type': 'none', 'value_exp_types': ['agg_exp'], 'distinct_type': 'none', 'min_max_depth_in_subquery': [1, 1]
            },
        }
    }
    # }
    



    generate_query_using_llm(
        # db_name="farm",
        # specs=specs,
        max_num=2,
        write_to_csv=True,
        random_choice=True,
        config_name="config_file.json",
        n_dbs = 21
    )
   