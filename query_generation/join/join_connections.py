import itertools
import random

import networkx as nx
from helper_funcs import create_graph_from_schema, generate_random_alias_name

# TODO self join


def get_max_joins_and_join_definitions(schema, fk):
    """
    Get the maximum number of joins possible and the join definitions.
    """

    join_definitions = create_graph_from_schema(schema, fk)
    max_num_joins = find_max_joins(join_definitions)
    return max_num_joins, join_definitions


def find_max_joins(join_definitions):
    """
    Find the maximum number of joins possible.

    Args:
        join_definitions (list): List of join definitions.

    Returns:
        int: The maximum number of joins possible.

    Examples:
        >>> join_definitions = [
        ...     {"table1": "A", "table2": "B"},
        ...     {"table1": "B", "table2": "C"},
        ...     {"table1": "C", "table2": "D"},
        ...     {"table1": "E", "table2": "F"},
        ... ]
        >>> find_max_joins(join_definitions)
        3

        >>> join_definitions = [
        ...     {"table1": "A", "table2": "B"},
        ...     {"table1": "C", "table2": "D"},
        ...     {"table1": "E", "table2": "F"},
        ...     {"table1": "G", "table2": "H"},
        ...     {"table1": "I", "table2": "J"},
        ... ]
        >>> find_max_joins(join_definitions)
        1
    """
    G = nx.Graph()
    for join in join_definitions:
        table1 = join["table1"]
        table2 = join["table2"]
        G.add_edge(table1, table2)
    return max(len(component) - 1 for component in nx.connected_components(G))


def generate_connections(join_definitions, num_tables):
    """
    Generate connections between tables based on the number of joins.

    Args:
        join_definitions (list): List of join definitions.
        num_tables (int): Number of tables for join to generate connections for.

    Returns:
        list: List of connections between tables.

    Examples:
        join_definitions = [
        ...     {"table1": "farm_competition", "table2": "city", "first_key": "Host_city_ID", "second_key": "City_ID"},
        ...     {"table1": "farm_competition", "table2": "competition_record", "first_key": "Competition_ID", "second_key": "Competition_ID"},
        ...     {"table1": "competition_record", "table2": "farm", "first_key": "Farm_ID", "second_key": "Farm_ID"},
        ... ]
        >>> generate_connections(join_definitions, 3)
        [['farm_competition', 'city', 'Host_city_ID', 'City_ID', 'farm_competition', 'competition_record', 'Competition_ID', 'Competition_ID'], ['farm_competition', 'competition_record', 'Competition_ID', 'Competition_ID', 'competition_record', 'farm', 'Farm_ID', 'Farm_ID']]
        >>> join_definitions = [
        ...     {"table1": "farm_competition", "table2": "city", "first_key": "Host_city_ID", "second_key": "City_ID"},
        ...     {"table1": "farm_competition", "table2": "competition_record", "first_key": "Competition_ID", "second_key": "Competition_ID"},
        ...     {"table1": "competition_record", "table2": "farm", "first_key": "Farm_ID", "second_key": "Farm_ID"},
        ... ]
        >>> generate_connections(join_definitions, 2)
        [['farm_competition', 'city', 'Host_city_ID', 'City_ID'], ['farm_competition', 'competition_record', 'Competition_ID', 'Competition_ID'], ['competition_record', 'farm', 'Farm_ID', 'Farm_ID']]

    """
    G = nx.Graph()
    G.add_edges_from((join["table1"], join["table2"]) for join in join_definitions)

    connections = []
    for combination in itertools.combinations(G.nodes, num_tables):
        subgraph = G.subgraph(combination)
        if nx.is_connected(subgraph):
            connection = [
                join[key]
                for join in join_definitions
                if join["table1"] in combination and join["table2"] in combination
                for key in ("table1", "table2", "first_key", "second_key")
            ]

            connections.append(connection)
    return connections


def generate_join_query(schema, fk, join_types, pk, random_choice=False, CTE=None):
    """
    Generate SQL join queries based on the specified number of joins and join types.

    Args:
        schema (str): The schema for the join queries.
        fk (str): The foreign key for the join queries.
        join_types (list): List of join types.
        random_choice (bool, optional): Whether to select a random connection. Defaults to False.

    Returns:
        list: List of generated join queries.

    Examples:
        >>> schema = #
        >>> fk = #
        >>> join_types = ["INNER JOIN",]
        >>> generate_join_query(schema, fk, join_types, random_choice=False)
        [[' FROM table1 INNER JOIN table2 ON table1.key = table2.key', ['table1', 'table2']]]

    """
    num_joins = len(join_types)
    join_definitions = create_graph_from_schema(schema, fk)
    print("join_definitions", join_definitions)
    len_self_join = 0
    join_types = sorted(join_types, key=lambda x: x == "SELF JOIN")
    alias_names = []

    if len(set(join_types)) == 1 and "SELF JOIN" in join_types:
        print("SELF JOIN")
        connections = []
        if not pk:
            raise ValueError("There is no Primary Key in the schema")
        for table in schema:

            if table in pk:
                print("table", table)
                print("pk", pk)
                len_infor = len(join_types) * 4
                connection = ["" for i in range(len_infor)]
                i = 0
                connection[i] = table
                connection[i + 2] = pk[table]
                for _ in range(len(join_types)):
                    alias_name = generate_random_alias_name(alias_names)
                    alias_names.append(alias_name)
                    connection[i + 1] = [alias_name, table]
                    connection[i + 3] = f"{alias_name}.{pk[table]}"
                    i += 4
                for i in range(len(connection)):
                    if connection[i] == "":
                        connection[i] = connection[i - 3]

                connections.append(connection)
                print("connections", connections)
    

    elif "SELF JOIN" in join_types:
        print("SELF JOIN")
        for e in join_types:
            if e == "SELF JOIN":
                len_self_join += 1
                num_joins -= 1

        connections = generate_connections(join_definitions, num_joins + 1)
        len_connections = len(connections)

        for i in range(len_connections):
            len_tables_so_far = len(connections[0]) // 4 + 1
            temp = connections[0].copy()
            temp1 = connections[0].copy()
            connections.remove(connections[0])
            unique_tables = set()

            for j in range(len(temp) // 4):
                connection = temp
                table1 = connection[j * 4]
                table2 = connection[j * 4 + 1]
                key1 = connection[j * 4 + 2]
                key2 = connection[j * 4 + 3]
                unique_tables.add((table1, key1))
                unique_tables.add((table2, key2))
            temp_index = 0
            for table, key in unique_tables:
                temp2 = temp1.copy()
                index = temp2.index(table) - 1

                if index != -1 and index != len(unique_tables) - 2:
                    continue
                if index == len(unique_tables) - 2:
                    index = index + 3

                for k in range(len_self_join):
                    alias_name = generate_random_alias_name(alias_names)
                    alias_names.append(alias_name)
                    temp2.insert(index + 1, table)
                    temp2.insert(index + 2, [alias_name, table])
                    temp2.insert(index + 3, pk[table])
                    temp2.insert(index + 4, f"{alias_name}.{pk[table]}")
                    # temp_index += 1

                connections.append(temp2)

            if i == "SELF JOIN":
                num_joins -= 1

        # def sort_connections(connections):

        # connections = sort_connections(connections)

    else:
        connections = generate_connections(join_definitions, num_joins + 1)
    print("connections", connections)
    if random_choice:
        connections = [random.choice(connections)]
    print("connections", connections)
    unique_tables_for_query = set()
    queries = []
    for connection in connections:
        tables = connection[::4]  # Extract table names
        join_conditions = []
        unique_tables = set()  # Use a set to ensure unique table names
        for j in range(len(connection) // 4):
            table1 = connection[j * 4]
            table2 = connection[j * 4 + 1]
            key1 = connection[j * 4 + 2]
            key2 = connection[j * 4 + 3]

            if isinstance(table1, list):
                if isinstance(table2, list):
                    join_conditions.append(f"{key1} = {key2}")
                    unique_tables.add(f"{table1[1]} AS {table1[0]}")
                    unique_tables_for_query.add(f"{table1[0]}@{table1[1]}")
                    unique_tables.add(f"{table2[1]} AS {table2[0]}")
                    unique_tables_for_query.add(f"{table2[0]}@{table2[1]}")
                else:
                    join_conditions.append(f"{key1} = {table2}.{key2}")
                    unique_tables.add(f"{table1[1]} AS {table1[0]}")
                    unique_tables_for_query.add(f"{table1[0]}@{table1[1]}")
                    if (
                        table2 in unique_tables
                        and list(unique_tables).index(table2) != 0
                    ):
                        unique_tables.remove(table2)
                        unique_tables_for_query.remove(table2)
                    unique_tables.add(table2)
                    unique_tables_for_query.add(table2)

            elif isinstance(table2, list):
                if isinstance(table1, list):
                    join_conditions.append(f"{key1} = {key2}")
                    unique_tables.add(f"{table1[1]} AS {table1[0]}")
                    unique_tables_for_query.add(f"{table1[0]}@{table1[1]}")
                    unique_tables.add(f"{table2[1]} AS {table2[0]}")
                    unique_tables_for_query.add(f"{table2[0]}@{table2[1]}")
                else:
                    join_conditions.append(f"{table1}.{key1} = {key2}")
                    if (
                        table1 in unique_tables
                        and list(unique_tables).index(table1) != 0
                    ):
                        unique_tables.remove(table1)
                        unique_tables_for_query.remove(table1)

                    unique_tables.add(table1)
                    unique_tables_for_query.add(table1)
                    unique_tables.add(f"{table2[1]} AS {table2[0]}")
                    unique_tables_for_query.add(f"{table2[0]}@{table2[1]}")
            else:
                join_conditions.append(f"{table1}.{key1} = {table2}.{key2}")
                if table1 not in unique_tables:
                    unique_tables.add(table1)
                    unique_tables_for_query.add(table1)
                if table2 not in unique_tables:
                    unique_tables.add(table2)
                    unique_tables_for_query.add(table2)
        print("HIII")
        join_clause = ""
        temp_index = 0
        unique_tables = list(unique_tables)

        for table in unique_tables:
            if temp_index == len(join_types):
                join_clause += f"{table}"
            else:
                modified_join_types = join_types[temp_index]

                if join_types[temp_index] == "SELF JOIN":
                    modified_join_types = "JOIN"

                join_clause += f"{table} {modified_join_types} "
                temp_index += 1

        on_clause = " AND ".join(join_conditions)
        query = f" FROM {join_clause} ON {on_clause}"
        unique_tables_for_query2 = list(unique_tables_for_query)

        new_unique_tables_for_query = []
        for table in unique_tables_for_query2:
            if "@" in table:
                new_unique_tables_for_query.append(
                    {table.split("@")[0]: table.split("@")[1]}
                )
            else:
                new_unique_tables_for_query.append(table)

        queries.append([query, list(new_unique_tables_for_query)])
    return queries


def generate_meaningless_join(
    schema, num_joins, join_types, num_queries=5, random_choice=False
):
    """
    Generate meaningless SQL join queries based on the specified number of joins and join types.

    Args:
        schema (dict): Dictionary representing the schema with table names as keys and column names as values.
        num_joins (int): Number of joins to generate in each query.
        join_types (list): List of join types.
        num_queries (int, optional): Number of queries to generate. Defaults to 5.
        random_choice (bool, optional): Whether to select a random connection. Defaults to False.

    Returns:
        list: List of generated join queries.

    Raises:
        ValueError: If the number of joins is less than 1 or there are not enough tables to perform the requested number of joins.

    Examples:
        >>> schema = {
        ...     "table1": ["col1", "col2"],
        ...     "table2": ["col3", "col4"],
        ...     "table3": ["col5", "col6"],
        ... }
        >>> num_joins = 1
        >>> join_types = ["INNER JOIN"]
        >>> generate_meaningless_join(schema, num_joins, join_types, num_queries=2, random_choice=True)
        [[' FROM table1 INNER JOIN table2 ON table1.col1 = table2.col3', ['table1', 'table2']], [' FROM table2 INNER JOIN table3 ON table2.col3 = table3.col5', ['table2', 'table3']]]
    """
    if num_joins < 1:
        raise ValueError("The number of joins must be at least 1")

    table_list = list(schema.keys())

    if len(table_list) < num_joins + 1:
        raise ValueError("Not enough tables to perform the requested number of joins")

    temp_queries = []
    if random_choice:
        num_queries = 1

    for _ in range(num_queries):
        tables = random.sample(table_list, num_joins + 1)

        join_conditions = []
        unique_tables = set()

        for j in range(num_joins):
            table1 = tables[j]
            table2 = tables[j + 1]

            key1 = random.choice(schema[table1])
            key2 = random.choice(schema[table2])

            unique_tables.add(table1)
            unique_tables.add(table2)

            join_conditions.append(f"{table1}.{key1} = {table2}.{key2}")

        join_clause = ""
        temp_index = 0
        for table in unique_tables:
            if temp_index == len(join_types):
                join_clause += f"{table}"
            else:
                modified_join_types = join_types[temp_index]

                if join_types[temp_index] == "SELF JOIN":
                    modified_join_types = "INNER JOIN"

                join_clause += f"{table} {modified_join_types} "
                temp_index += 1
        on_clause = " AND ".join(join_conditions)
        query = f" FROM {join_clause} ON {on_clause}"
        temp_queries.append([query, list(unique_tables)])

    return temp_queries


# Example usage
schema = {
    "table1": ["id", "name", "age"],
    "table2": ["id", "address", "phone"],
    "table3": ["id", "product", "price"],
}
schema_types = {}  # You can add schema types as needed
num_joins = 2
meaningless_join_query = generate_meaningless_join(schema, num_joins, ["INNER JOIN"])


schema = {
    "city": [
        "city_id",
        "official_name",
        "status",
        "area_km_2",
        "population",
        "census_ranking",
    ],
    "farm": [
        "farm_id",
        "year",
        "total_horses",
        "working_horses",
        "total_cattle",
        "oxen",
        "bulls",
        "cows",
        "pigs",
        "sheep_and_goats",
    ],
    "farm_competition": ["competition_id", "year", "theme", "host_city_id", "hosts"],
    "competition_record": ["competition_id", "farm_id", "rank"],
}
schema_types = {
    "city": {
        "city_id": "number",
        "official_name": "text",
        "status": "text",
        "area_km_2": "number",
        "population": "number",
        "census_ranking": "text",
    },
    "farm": {
        "farm_id": "number",
        "year": "number",
        "total_horses": "number",
        "working_horses": "number",
        "total_cattle": "number",
        "oxen": "number",
        "bulls": "number",
        "cows": "number",
        "pigs": "number",
        "sheep_and_goats": "number",
    },
    "farm_competition": {
        "competition_id": "number",
        "year": "number",
        "theme": "text",
        "host_city_id": "number",
        "hosts": "text",
    },
    "competition_record": {
        "competition_id": "number",
        "farm_id": "number",
        "rank": "number",
    },
}

fk = {
    "farm_competition": {"host_city_id": ("city", "city_id")},
    "competition_record": {
        "farm_id": ("farm", "farm_id"),
        "competition_id": ("farm_competition", "competition_id"),
    },
}
