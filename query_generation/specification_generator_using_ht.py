import json
import os
import random
import ast

from helper_funcs import calculate_hash, select_combinations, write_hash_table_to_json
from join import get_max_joins_and_join_definitions
from read_schema.read_schema import convert_json_to_schema

FROM_WEIGHTS= {
    'single_table': 2.0,
    'single_table_with_name_changing': 1.5,
    'JOIN': 1.2,
    'SELF JOIN': .5,
    'JOIN_SELF JOIN': 0,
    'JOIN_JOIN': 0.6,
    'SELF JOIN_SELF JOIN': 0,
    'JOIN_JOIN_JOIN':0.5,
    'JOIN_JOIN_JOIN_JOIN':0.2,
    'JOIN_JOIN_SELF JOIN': 0,
    'SELF JOIN_SELF JOIN_SELF JOIN': 0,
    'JOIN_SELF JOIN_SELF JOIN': 0,

    # 'JOIN_JOIN_JOIN_JOIN':0.2

}
current_sum = sum(FROM_WEIGHTS.values())

# Scale down each probability
for key in FROM_WEIGHTS:
    FROM_WEIGHTS[key] /= current_sum

WHERE_WEIGHTS =  {
    'none': 0.2,
    'between': 0.1,
    str({'basic_comparison': '='}): 0.15,
    str({'basic_comparison': '!='}): 0.15,
    str({'basic_comparison': '>'}): 0.15,
    str({'basic_comparison': '<'}): 0.15,
    str({'basic_comparison': '>='}): 0.15,
    str({'basic_comparison': '<='}): 0.15,
    'in_with_subquery': 0.05,
    'not_in_with_subquery': 0.05,
    'comparison_with_subquery': 0.05,
    'not_exists_subquery': 0.05,
    'exists_subquery': 0.05,
    str({'logical_operator': ['AND', 'between', 'basic_comparison']}): 0.1,
    str({'logical_operator': ['OR', 'between', 'basic_comparison']}): 0.1,
    str({'logical_operator': ['AND', 'basic_comparison', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'exists_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'basic_comparison', 'exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'basic_comparison', 'exists_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'between', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'between', 'in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'between', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'between', 'not_in_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'between', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'between', 'comparison_with_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'between', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'between', 'not_exists_subquery']}): 0.02,
    str({'logical_operator': ['AND', 'between', 'exists_subquery']}): 0.02,
    str({'logical_operator': ['OR', 'between', 'exists_subquery']}): 0.02
}
current_sum = sum(WHERE_WEIGHTS.values())

# Scale down each probability
for key in WHERE_WEIGHTS:
    WHERE_WEIGHTS[key] /= current_sum


GROUP_BY_WEIGHTS = {
    0: 0.6,
    1: 0.3,
    2: 0.1
}
HAVING_WEIGHTS = {
    str({'single': 'MAX'}): 0.1,
    str({'single': 'MIN'}): 0.1,
    str({'single': 'AVG'}): 0.1,
    str({'single': 'SUM'}): 0.1,
    str({'single': 'COUNT'}): 0.1,
    'none': 0.5
}


ORDER_BY_WEIGHTS = {
    'ASC': 0.2,
    'DESC': 0.2,
    'none': 0.6
}
LIMIT_WEIGHTS = {
    'none': 0.7,
    'without_offset': 0.3
}
VALUE_EXP_WEIGHTS = {
    '*': 0.2,
    str(['count_distinct_exp']): 0.1,
    str(['arithmatic_exp']): 0.1,
    str(['single_exp_number']): 0.2,
    str(['agg_exp']): 0.1,
    str(['alias_exp']): 0.08,
    str(['single_exp_text']): 0.2,
    str(['single_exp_time']): 0.2,
    str(['agg_exp', 'count_distinct_exp']): 0.05,
    str(['arithmatic_exp', 'count_distinct_exp']): 0.05,
    str(['count_distinct_exp', 'count_distinct_exp']): 0.01,
    str(['alias_exp', 'count_distinct_exp']): 0.05,
    str(['agg_exp', 'arithmatic_exp']): 0.1,
    str(['agg_exp', 'agg_exp']): 0.05,
    str(['agg_exp', 'single_exp_text']): 0.1,
    str(['agg_exp', 'alias_exp']): 0.1,
    str(['count_distinct_exp', 'single_exp_text']): 0.05,
    str(['arithmatic_exp', 'arithmatic_exp']): 0.1,
    str(['arithmatic_exp', 'single_exp_text']): 0.1,
    str(['single_exp_text', 'single_exp_text']): 0.1,
    str(['agg_exp', 'single_exp_number']): 0.1,
    str(['arithmatic_exp', 'single_exp_number']): 0.1,
    str(['alias_exp', 'arithmatic_exp']): 0.1,
    str(['count_distinct_exp', 'single_exp_number']): 0.05,
    str(['alias_exp', 'single_exp_text']): 0.1,
    str(['alias_exp', 'alias_exp']): 0.05,
    str(['single_exp_number', 'single_exp_text']): 0.1,
    str(['single_exp_number', 'single_exp_number']): 0.1,
    str(['alias_exp', 'single_exp_number']): 0.1,
    str(['count_distinct_exp', 'count_distinct_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'alias_exp', 'single_exp_number']): 0.01,
    str(['alias_exp', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['arithmatic_exp', 'count_distinct_exp', 'single_exp_number']): 0.01,
    str(['agg_exp', 'alias_exp', 'count_distinct_exp']): 0.01,
    str(['count_distinct_exp', 'count_distinct_exp', 'single_exp_number']): 0.01,
    str(['alias_exp', 'count_distinct_exp', 'count_distinct_exp']): 0.01,
    str(['count_distinct_exp', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['alias_exp', 'arithmatic_exp', 'count_distinct_exp']): 0.01,
    str(['agg_exp', 'agg_exp', 'arithmatic_exp']): 0.01,
    str(['arithmatic_exp', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['agg_exp', 'agg_exp', 'agg_exp']): 0.01,
    str(['agg_exp', 'agg_exp', 'alias_exp']): 0.01,
    str(['agg_exp', 'agg_exp', 'single_exp_text']): 0.01,
    str(['arithmatic_exp', 'arithmatic_exp', 'count_distinct_exp']): 0.01,
    str(['agg_exp', 'count_distinct_exp', 'single_exp_text']): 0.01,
    str(['agg_exp', 'arithmatic_exp', 'arithmatic_exp']): 0.01,
    str(['count_distinct_exp', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['agg_exp', 'arithmatic_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'alias_exp', 'count_distinct_exp']): 0.01,
    str(['agg_exp', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['arithmatic_exp', 'count_distinct_exp', 'count_distinct_exp']): 0.01,
    str(['arithmatic_exp', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['single_exp_number', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['agg_exp', 'agg_exp', 'single_exp_number']): 0.01,
    str(['single_exp_text', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['agg_exp', 'count_distinct_exp', 'single_exp_number']): 0.01,
    str(['count_distinct_exp', 'count_distinct_exp', 'count_distinct_exp']): 0.01,
    str(['agg_exp', 'alias_exp', 'single_exp_text']): 0.01,
    str(['count_distinct_exp', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['alias_exp', 'count_distinct_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'arithmatic_exp', 'arithmatic_exp']): 0.01,
    str(['agg_exp', 'alias_exp', 'arithmatic_exp']): 0.01,
    str(['agg_exp', 'arithmatic_exp', 'single_exp_number']): 0.01,
    str(['agg_exp', 'alias_exp', 'alias_exp']): 0.01,
    str(['alias_exp', 'arithmatic_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['agg_exp', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['arithmatic_exp', 'arithmatic_exp', 'arithmatic_exp']): 0.01,
    str(['single_exp_number', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['arithmatic_exp', 'arithmatic_exp', 'single_exp_text']): 0.01,
    str(['agg_exp', 'alias_exp', 'single_exp_number']): 0.01,
    str(['alias_exp', 'count_distinct_exp', 'single_exp_number']): 0.01,
    str(['alias_exp', 'alias_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'arithmatic_exp', 'single_exp_number']): 0.01,
    str(['alias_exp', 'alias_exp', 'arithmatic_exp']): 0.01,
    str(['alias_exp', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['arithmatic_exp', 'count_distinct_exp', 'single_exp_text']): 0.01,
    str(['alias_exp', 'alias_exp', 'alias_exp']): 0.01,
    str(['agg_exp', 'agg_exp', 'count_distinct_exp']): 0.01,
    str(['agg_exp', 'count_distinct_exp', 'count_distinct_exp']): 0.01,
    str(['arithmatic_exp', 'arithmatic_exp', 'single_exp_number']): 0.01,
    str(['agg_exp', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['agg_exp', 'arithmatic_exp', 'count_distinct_exp']): 0.01,
    str(['single_exp_number', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['arithmatic_exp', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'agg_exp']): 0.1,
    str(['single_exp_time', 'arithmatic_exp']): 0.1,
    str(['single_exp_time', 'single_exp_number']): 0.1,
    str(['single_exp_time', 'single_exp_text']): 0.1,
    str(['single_exp_time', 'alias_exp']): 0.1,
    str(['single_exp_time', 'count_distinct_exp']): 0.05,
    str(['single_exp_time', 'agg_exp', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'arithmatic_exp', 'single_exp_number']): 0.01,
    str(['single_exp_time', 'count_distinct_exp', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'alias_exp', 'single_exp_number']): 0.01,
    str(['single_exp_time', 'agg_exp', 'arithmatic_exp']): 0.01,
    str(['single_exp_time', 'agg_exp', 'single_exp_number']): 0.01,
    str(['single_exp_time', 'arithmatic_exp', 'count_distinct_exp']): 0.01,
    str(['single_exp_time', 'alias_exp', 'arithmatic_exp']): 0.01,
    str(['single_exp_time', 'alias_exp', 'count_distinct_exp']): 0.01,
    str(['single_exp_time', 'single_exp_number', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'agg_exp', 'alias_exp']): 0.01,
    str(['single_exp_time', 'agg_exp', 'count_distinct_exp']): 0.01,
    str(['single_exp_time', 'single_exp_text', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'single_exp_number', 'single_exp_number']): 0.01,
    str(['single_exp_time', 'alias_exp', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'arithmatic_exp', 'single_exp_text']): 0.01,
    str(['single_exp_time', 'arithmatic_exp', 'agg_exp']): 0.01
}
current_sum = sum(VALUE_EXP_WEIGHTS.values())

# Scale down each probability
for key in VALUE_EXP_WEIGHTS:
    VALUE_EXP_WEIGHTS[key] /= current_sum



def normalize_weights(subset_options,all_weights):
    """
    Normalize the weights for a given subset of options.

    Parameters:
    - all_weights (dict): Dictionary containing weights for all options.
    - subset_options (list): List of subset options for which to normalize weights.

    Returns:
    - dict: Dictionary of normalized weights for the subset options.
    """


    # Extract the weights for the subset options
 
    if type(subset_options[0]) == int:
        subset_weights = {option: all_weights[option] for option in subset_options}
    else:
        subset_weights = {str(option): all_weights[str(option)] for option in subset_options}

    # Calculate the total weight of the subset
    total_weight = sum(subset_weights.values())

    # Normalize the weights
    normalized_weights = {option: weight / total_weight for option, weight in subset_weights.items()}


    return list( normalized_weights.keys()),list(normalized_weights.values())
def complete_specs(db_file, config_file, db_name=None, num_query=1000):
    """
    Generate specifications for queries based on the given database schema and configuration.

    Args:
        db_file (str): The path to the JSON file containing the database schema.
        config_file (str): The path to the JSON file containing the configuration for generating specifications.
        db_name (str, optional): The name of the specific database to generate specifications for. Defaults to None.

    Returns:
        None
    """
    all_db = convert_json_to_schema(db_file)
    specs = {}

    with open(config_file, "r") as f:
        spec_config = json.load(f)

    if db_name:
        print(db_name)
        specs[db_name] = generate_specifications_for_queries(
            all_db[db_name]["schema"],
            all_db[db_name]["foreign_keys"],
            spec_config,
            num=num_query,
        )

        current_dir = os.path.dirname(__file__)
        file_name = os.path.join(current_dir, f"output/specs/{db_name}.json")
        write_hash_table_to_json(specs, file_name)
    else:
        for db in all_db:
            specs[db_name] = generate_specifications_for_queries(
                all_db[db_name]["schema"],
                all_db[db_name]["foreign_keys"],
                spec_config,
                num=num_query,
            )

            current_dir = os.path.dirname(__file__)
            file_name_for_write = os.path.join(current_dir, f"output/specs/{db}.json")
            write_hash_table_to_json(specs[db], file_name_for_write)


def generate_specifications_for_queries(schema, foreign_keys, specs, num=1000):
    set_ops_types = specs["set_op_types"]
    try:
        first_spec = generate_specifications_for_queries_without_set_ops(
            schema, foreign_keys, specs["first_query"], num
        )
    except Exception as e:
        print(e)
        return

    if "second_query" in specs:
        try:
            second_spec = generate_specifications_for_queries_without_set_ops(
                schema, foreign_keys, specs["second_query"], num
            )
        except Exception as e:
            print(e)
            return

    hash_table = {}

    for _ in range(num):
        set_op_type = random.choice(set_ops_types)
        if set_op_type == "none":
            detail = {
                
                "set_op_type": set_op_type,
                "first_query": first_spec[random.choice(list(first_spec))],
            }
        else:
            spec1 = first_spec[random.choice(list(first_spec))]
            spec2 = second_spec[random.choice(list(second_spec))]

            if (
                spec1["number_of_value_exp_in_group_by"] != 0
                or spec2["number_of_value_exp_in_group_by"] != 0
            ):
                spec2["number_of_value_exp_in_group_by"] = 0
                spec1["number_of_value_exp_in_group_by"] = 0
                spec1["having_type"] = "none"
                spec2["having_type"] = "none"
            # not *
            spec1["min_max_depth_in_subquery"] = [0, 0]
            spec2["min_max_depth_in_subquery"] = [0, 0]
            spec1["value_exp_types"] = spec2["value_exp_types"]
            spec1["limit_type"] = spec2["limit_type"] = "none"

            detail = {
                "set_op_type": set_op_type,
                "first_query": spec1,
                "second_query": spec2,
            }
        hash_value = calculate_hash(detail)

        if hash_value not in hash_table:
            hash_table[hash_value] = detail

    return hash_table


def generate_specifications_for_queries_without_set_ops(
    schema, foreign_keys, specs, num=100
):
    table_exp_types = specs.get("table_exp_types", [])
    where_clause_types = specs.get("where_clause_types", [])

    pattern_matching_types, like_or_not_like = handle_pattern_matching(
        specs, where_clause_types
    )
    subquery_in_where = handle_subquery_in_where(specs, where_clause_types)
    basic_comp_ops = handle_basic_comparison(specs, where_clause_types)
    null_operators = handle_null_check(specs, where_clause_types)
    in_set = handle_in_set(specs, where_clause_types)
    join_types, meaningful_joins = handle_join_types(specs, table_exp_types)
    (
        having_types_with_having_group_by,
        aggregate_functions_for_having,
        subquery_in_having,
        number_of_value_exps_in_group_by,
    ) = handle_group_by_and_having(specs)
    (
        number_of_value_exps_in_select,
        value_exp_types,
        distinct_types,
        string_func_exp_types,
        arithmatic_exp_types,
        agg_exp_types,
    ) = handle_select_clause(specs)
    min_max_depth_in_subquery = specs.get("min_max_depth_in_subquery", [0, 0])
    orderby_types = specs.get("orderby_types", ["none"])
    limit_types = specs["limit_types"]
    table_exp_types_with_types_of_joins = generate_table_expression_types(
        meaningful_joins, join_types, table_exp_types, schema, foreign_keys
    )
    if table_exp_types_with_types_of_joins == ["self_join"]:
        meaningful_joins = ["yes"]
    completed_specifications = {
        "table_exp_types": table_exp_types_with_types_of_joins,
        "where_clause_types": generate_where_clause_types(
            where_clause_types,
            null_operators,
            basic_comp_ops,
            pattern_matching_types,
            like_or_not_like,
            in_set,
            subquery_in_where=subquery_in_where,
        ),
    }
    if "logical_operators" in where_clause_types:
        add_logical_operator_combinations(
            completed_specifications["where_clause_types"]
        )
       

    completed_specifications["number_of_value_exps_in_group_by"] = (
        number_of_value_exps_in_group_by
    )
    completed_specifications["having_types_with_having_group_by"] = (
        generate_having_types(
            having_types_with_having_group_by,
            aggregate_functions_for_having,
            subquery_in_having,
        )
    )
    having_types_with_having_group_by = completed_specifications[
        "having_types_with_having_group_by"
    ]

    having_types_without_having_group_by = ["none"]
    all_value_exp_types = generate_all_value_exp_types(
        value_exp_types,
        agg_exp_types,
        string_func_exp_types,
        arithmatic_exp_types,
        number_of_value_exps_in_select,
    )

    return generate_hash_table(
        num,
        table_exp_types_with_types_of_joins,
        completed_specifications["where_clause_types"],
        number_of_value_exps_in_group_by,
        having_types_without_having_group_by,
        having_types_with_having_group_by,
        orderby_types,
        limit_types,
        meaningful_joins,
        distinct_types,
        all_value_exp_types,
        min_max_depth_in_subquery,
    )


def handle_pattern_matching(specs, where_clause_types):
    if "pattern_matching" in where_clause_types and (
        "like_or_not_like" not in specs or "pattern_matching_types" not in specs
    ):
        raise Exception("Specify like_or_not_like and pattern_matching_types")
    pattern_matching_types = specs.get("pattern_matching_types", [])
    like_or_not_like = specs.get("like_or_not_like", [])
    return pattern_matching_types, like_or_not_like


def handle_basic_comparison(specs, where_clause_types):
    if "basic_comparison" in where_clause_types and "basic_comp_ops" not in specs:
        raise Exception("Specify basic_comp_ops")
    return specs.get("basic_comp_ops", [])


def handle_subquery_in_where(specs, where_clause_types):
    if "subquery" in where_clause_types and "subquery_in_where" not in specs:
        raise Exception("Specify subquery_in_where")
    return specs.get("subquery_in_where", [])


def handle_null_check(specs, where_clause_types):
    if "null_check" in where_clause_types and "null_operators" not in specs:
        raise Exception("Specify null_operators")
    return specs.get("null_operators", [])


def handle_in_set(specs, where_clause_types):
    if "in_set" in where_clause_types and "in_set" not in specs:
        raise Exception("Specify in_set")
    return specs.get("in_set", [])


def handle_join_types(specs, table_exp_types):
    if any(i.startswith("join") for i in table_exp_types) and (
        "join_types" not in specs or "meaningful_joins" not in specs
    ):
        raise Exception("Specify join_types and meaningful_joins")
    join_types = specs.get("join_types", [])
    meaningful_joins = specs.get("meaningful_joins", [])
    return join_types, meaningful_joins


def handle_group_by_and_having(specs):
    if specs["number_of_value_exps_in_group_by"] != [0]:
        if "having_types" not in specs or "aggregate_functions_for_having" not in specs:
            raise Exception("Specify having_types and aggregate_functions_for_having")
        if "subquery" in specs["having_types"] and "subquery_in_having" not in specs:
            raise Exception("Specify subquery_in_having")
    subquery_in_having = specs.get("subquery_in_having", [])
    having_types = specs.get("having_types", [])
    aggregate_functions_for_having = specs.get("aggregate_functions_for_having", [])
    number_of_value_exps_in_group_by = specs.get("number_of_value_exps_in_group_by", [])
    return (
        having_types,
        aggregate_functions_for_having,
        subquery_in_having,
        number_of_value_exps_in_group_by,
    )


def handle_select_clause(specs):
    number_of_value_exps_in_select = specs.get("number_of_value_exps_in_select", [])
    value_exp_types = specs.get("value_exp_types", [1])
    distinct_types = specs.get("distinct_types", ["none"])
    string_func_col = specs.get("string_func_col", ["no_alias"])
    arithmatic_col = specs.get("arithmatic_col", ["no_alias"])
    agg_col = specs.get("agg_col", ["no_alias"])
    return (
        number_of_value_exps_in_select,
        value_exp_types,
        distinct_types,
        string_func_col,
        arithmatic_col,
        agg_col,
    )


def generate_table_expression_types(
    meaningful_joins, join_types, table_exp_types, schema, foreign_keys
):
    """
    Generate the table expression types for the specifications.

    Args:
        meaningful_joins (str): The flag indicating whether meaningful joins are enabled or not.
        join_types (list): The list of join types.
        table_exp_types (list): The list of table expression types.
        schema (dict): The schema dictionary representing the tables and their columns.
        foreign_keys (dict): The foreign keys dictionary representing the tables and their foreign key relationships.

    Returns:
        list: The generated table expression types with types of joins.
    """

    if meaningful_joins == "yes":
        max_joins, _ = get_max_joins_and_join_definitions(schema, foreign_keys)
        table_exp_types = [
            table_exp_type
            for table_exp_type in table_exp_types
            if not table_exp_type.startswith("join")
            or int(table_exp_type.split("_")[1]) <= max_joins
        ]

    table_exp_types_with_types_of_joins = []
    for table_exp_type in table_exp_types:
        if table_exp_type.startswith("join"):
            _, join_num = table_exp_type.split("_")
            type_of_joins = select_combinations(join_types, int(join_num))
            table_exp_types_with_types_of_joins.extend(
                "_".join(type_of_join) for type_of_join in type_of_joins
            )
        else:
            table_exp_types_with_types_of_joins.append(table_exp_type)
    return table_exp_types_with_types_of_joins


def generate_where_clause_types(
    where_clause_types,
    null_operators,
    basic_comp_ops,
    pattern_matching_types,
    like_or_not_like,
    in_set,
    subquery_in_where=None,
):
    """
    Generate the where clause types for the specifications.

    Args:
        where_clause_types (list): The list of where clause types.
        null_operators (list): The list of null operators.
        basic_comp_ops (list): The list of basic comparison operators.
        pattern_matching_types (list): The list of pattern matching types.
        like_or_not_like (list): The list of like or not like operators.
        in_set (list): The list of in set types.
        subquery_in_where (list): The list of subquery in where types.

    Returns:
        list: The generated where clause types.
    """
    generated_where_clause_types = []
    for where_type in where_clause_types:
        if where_type in ["none", "between"]:
            generated_where_clause_types.append(where_type)
        elif where_type == "null_check":
            generated_where_clause_types.extend(
                {"null_check": item} for item in null_operators
            )
        elif where_type == "basic_comparison":
            generated_where_clause_types.extend(
                {"basic_comparison": item} for item in basic_comp_ops
            )
        elif where_type == "pattern_matching":
            for like_op in like_or_not_like:
                generated_where_clause_types.extend(
                    {"pattern_matching": [like_op, criteria]}
                    for criteria in pattern_matching_types
                )
        elif where_type == "in_set":
            generated_where_clause_types.extend(in_set)
        elif where_type == "subquery":
            generated_where_clause_types.extend(subquery_in_where)

    return generated_where_clause_types


def add_logical_operator_combinations(where_clause_types):
    """
    Add logical operator combinations to the where clause types.

    Args:
        where_clause_types (list): The list of where clause types.

    Returns:
        None
    """
    max_combination = len(where_clause_types)

    for i in range(max_combination):
        for j in range(i + 1, max_combination):
            if where_clause_types[i] == "none" or where_clause_types[j] == "none":
                continue
            if isinstance(where_clause_types[i], dict) and isinstance(
                where_clause_types[j], dict
            ):
                continue

            first_item = where_clause_types[i]
            second_item = where_clause_types[j]
            if isinstance(where_clause_types[i], dict):
                first_item = list(where_clause_types[i].keys())[0]
            if isinstance(where_clause_types[j], dict):
                second_item = list(where_clause_types[j].keys())[0]
            if "subquery" in first_item and "subquery" in second_item:
                continue

            where_clause_types.append(
                {"logical_operator": ["AND", first_item, second_item]}
            )

            where_clause_types.append(
                {"logical_operator": ["OR", first_item, second_item]}
            )


def generate_having_types(
    having_types_with_having_group_by,
    aggregate_functions_for_having,
    subquery_in_having,
):
    """
    Generate the having types for the specifications.

    Args:
        having_types_with_having_group_by (list): The list of having types with having group by.
        aggregate_functions_for_having (list): The list of aggregate functions for having.

    Returns:
        list: The generated having types.
    """
    generated_having_types = []

    if "single" in having_types_with_having_group_by:
        generated_having_types.extend(
            {"single": agg_func} for agg_func in aggregate_functions_for_having
        )
    if "multiple" in having_types_with_having_group_by:
        generated_having_types.append("multiple")

    if "none" in having_types_with_having_group_by:
        generated_having_types.append("none")
    if "subquery" in having_types_with_having_group_by:
        generated_having_types.extend(subquery_in_having)
    return generated_having_types


def generate_all_value_exp_types(
    value_exp_types,
    agg_func_col_types,
    string_func_col_types,
    arithmatic_col_types,
    number_of_value_exps_in_select,
):
    """
    Generate all value expression types for the specifications.

    Args:
        value_exp_types (list): The list of value expression types.
        agg_func_col_types (list): The list of aggregate function column types.
        string_func_col_types (list): The list of string function column types.
        arithmatic_col_types (list): The list of arithmetic column types.

    Returns:
        list: The generated value expression types.
    """

    all_value_exp_types = []
    if "subquery_exp" in value_exp_types:
        value_exp_types.remove("subquery_exp")
        value_exp_types.append("subquery_exp_alias")
    if "single_exp" in value_exp_types:
        value_exp_types.remove("single_exp")
        value_exp_types.append("single_exp_number")
        value_exp_types.append("single_exp_text")
    if agg_func_col_types:
        if "alias" in agg_func_col_types:
            value_exp_types.append("agg_exp_alias")
        if "no_alias" not in agg_func_col_types and "agg_exp" in value_exp_types:
            value_exp_types.remove("agg_exp")

    if string_func_col_types:
        if "alias" in string_func_col_types:
            value_exp_types.append("string_func_exp_alias")
        if (
            "no_alias" not in string_func_col_types
            and "string_func_exp" in value_exp_types
        ):
            value_exp_types.remove("string_func_exp")

    if arithmatic_col_types:
        if "alias" in arithmatic_col_types:
            value_exp_types.append("arithmatic_exp_alias")
        if (
            "no_alias" not in arithmatic_col_types
            and "arithmatic_exp" in value_exp_types
        ):
            value_exp_types.remove("arithmatic_exp")

    for i in number_of_value_exps_in_select:
        if i == "*":
            all_value_exp_types.append("*")
            continue
        all_combinations = select_combinations(value_exp_types, i)
        all_value_exp_types.extend(all_combinations)
    return all_value_exp_types


def generate_hash_table(
    num,
    table_exp_types_with_types_of_joins,
    where_clause_types,
    number_of_value_exps_in_group_by,
    having_types_without_having_group_by,
    having_types_with_having_group_by,
    orderby_types,
    limit_types,
    meaningful_joins,
    distinct_types,
    all_value_exp_types,
    min_max_depth_in_subquery,
):
    """
    Generate the hash table of specifications.

    Args:
        num (int): The number of specifications to generate.
        table_exp_types_with_types_of_joins (list): The list of table expression types with types of joins.
        where_clause_types (list): The list of where clause types.
        number_of_value_exps_in_group_by (list): The list of number of value expressions in group by.
        having_types_without_having_group_by (list): The list of having types without having group by.
        having_types_with_having_group_by (list): The list of having types with having group by.
        orderby_types (list): The list of orderby types.
        limit_types (list): The list of limit types.
        meaningful_joins (list): The list of meaningful joins.
        distinct_types (list): The list of distinct types.
        all_value_exp_types (list): The list of all value expression types.
        min_max_depth_in_subquery (list): The list of min and max depth in subquery.

    Returns:
        dict: The generated hash table of specifications.
    """
    hash_table = {}
    

    for _ in range(num):
        # table_exp_type = weighted_random_choice(
        #     table_exp_types_with_types_of_joins, [0.5, 0.5]
        # )
       
        from_items,from_weights = normalize_weights( table_exp_types_with_types_of_joins,FROM_WEIGHTS)
        table_exp_type = random.choices(from_items,from_weights ,k=1)[0]
        where_items, where_weights =  normalize_weights( where_clause_types,WHERE_WEIGHTS)


        where_type = random.choices( where_items, where_weights,k=1)[0]
        if where_type[0] == "{":
            where_type = ast.literal_eval(where_type)


        if table_exp_type == "CTE":
            if isinstance(where_type, dict):
                while "logical_operator" in where_type and (
                    "subquery" in where_type["logical_operator"][1]
                    or "subquery" in where_type["logical_operator"][2]
                ):
                    where_type = random.choices( where_items, where_weights,k=1)[0]
            else:
                while "subquery" in where_clause_types:
                    where_type = random.choices( where_items, where_weights,k=1)[0]

        group_by_items, group_by_weights = normalize_weights(number_of_value_exps_in_group_by, GROUP_BY_WEIGHTS)

        group_by_type = random.choices(group_by_items,group_by_weights,k=1)[0]

        having_items, having_weights = normalize_weights(having_types_with_having_group_by, HAVING_WEIGHTS)

        having_type = (
            random.choice(having_types_without_having_group_by)
            if group_by_type == 0
            else random.choices(having_items,having_weights,k=1)[0]
        )
        if having_type[0] == "{":
            having_type = ast.literal_eval(having_type)
        order_by_items, order_by_weights = normalize_weights(orderby_types, ORDER_BY_WEIGHTS)
        orderby_type = random.choices(order_by_items, order_by_weights,k=1)[0]
        limit_items, limit_weights = normalize_weights(limit_types, LIMIT_WEIGHTS)
        limit_type = random.choices( limit_items, limit_weights,k=1)[0]
        type_of_join = random.choice(meaningful_joins)
        if "SELF JOIN" in table_exp_type:
            type_of_join = "yes"
        distinct_type = random.choice(distinct_types)
        value_exp_items, value_exp_weights = normalize_weights(all_value_exp_types, VALUE_EXP_WEIGHTS)
        value_exp_type = random.choices(value_exp_items, value_exp_weights ,k=1)[0]
        if value_exp_type[0] == "[":
            value_exp_type = ast.literal_eval(value_exp_type)
        detail = {
            "meaningful_joins": type_of_join,
            "table_exp_type": table_exp_type,
            "where_type": where_type,
            "number_of_value_exp_in_group_by": group_by_type,
            "having_type": having_type,
            "orderby_type": orderby_type,
            "limit_type": limit_type,
            "value_exp_types": value_exp_type,
            "distinct_type": distinct_type,
            "min_max_depth_in_subquery": min_max_depth_in_subquery,
        }
       

        hash_value = calculate_hash(detail)

        if hash_value not in hash_table:
            hash_table[hash_value] = detail

    return hash_table


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    dataset_path = os.path.join(current_dir, "../data/tables.json")
    config_file = os.path.abspath(os.path.join(current_dir, "config_file.json"))

    complete_specs(
        dataset_path,
        config_file,
        db_name="advertising_agencies",
        num_query=30,
        
    )
