import json

if __name__ == "__main__":

    # Load the JSON data
    with open("train_spider.json", "r") as f:
        data = json.load(f)

    # Initialize counters
    num_queries = len(data)
    num_group_by = 0
    num_column_name_in_group_by = {}

    num_selects = {}
    num_orders = 0
    order_by_details = {"none": 0}
    num_where = {}
    num_having = {"none": 0}
    num_limits = {"has_limit": 0, "no_limit": 0}
    num_intersects = 0
    num_unions = 0
    num_excepts = 0
    where_details = {
        "logical_op": 0,
        "between": 0,
        "in": 0,
        "like": 0,
        "in_subquery": 0,
        "comparison_subquery": 0,
        "exists": 0,
        "null": 0,
        "none": 0,
    }
    from_details = {"subquery": 0}

    # Iterate over each query
    for query_data in data:
        sql = query_data["sql"]
        if sql["from"]:
            if "sql" in sql["from"]["table_units"][0]:
                from_details["subquery"] += 1
            else:
                n = len(sql["from"]["table_units"])
                if from_details.get(n):
                    from_details[n] += 1
                else:
                    from_details[n] = 1

        # Check if GROUP BY clause exists
        if sql["groupBy"]:
            num_group_by += 1
            if num_column_name_in_group_by.get(len(sql["groupBy"])):
                num_column_name_in_group_by[len(sql["groupBy"])] += 1
            else:
                num_column_name_in_group_by[len(sql["groupBy"])] = 1
        else:
            if num_column_name_in_group_by.get(0):
                num_column_name_in_group_by[0] += 1
            else:
                num_column_name_in_group_by[0] = 1

        # Count the number of SELECT statements
        if num_selects.get(len(sql["select"][1])):
            num_selects[len(sql["select"][1])] += 1
        else:
            num_selects[len(sql["select"][1])] = 1

        # Check if ORDER BY clause exists
        if sql["orderBy"]:

            n = len(sql["orderBy"][1])
            if order_by_details.get(n):
                order_by_details[n] += 1
            else:
                order_by_details[n] = 1

        else:
            order_by_details["none"] += 1

        # Count the number of WHERE clauses
        if sql["where"]:
            if "and" in sql["where"] or "or" in sql["where"]:
                where_details["logical_op"] += 1
            n = len(sql["where"])
            if n > 1:
                n = n // 2 + 1
            if num_where.get(n):
                num_where[n] += 1
            else:
                num_where[n] = 1
        if len(sql["where"]) > 0:
            if "BETWEEN" in query_data["query"]:
                where_details["between"] += 1
            if "LIKE" in query_data["query"]:
                where_details["like"] += 1
            if "EXISTS" in query_data["query"]:
                where_details["exists"] += 1
            if "NULL" in query_data["query"]:
                where_details["null"] += 1
            if (
                " < (" in query_data["query"]
                or " > (" in query_data["query"]
                or " = (" in query_data["query"]
                or " != (" in query_data["query"]
                or " <= (" in query_data["query"]
                or " >= (" in query_data["query"]
            ):
                where_details["comparison_subquery"] += 1
            if (
                " IN ( SELECT" in query_data["query"]
                or " IN (SELECT" in query_data["query"]
            ):
                where_details["in_subquery"] += 1

            elif " IN " in query_data["query"]:
                print(query_data["query"])
                where_details["in"] += 1

        else:
            where_details["none"] += 1

        # Count the number of HAVING clauses
        if sql["having"]:
            n = len(sql["having"])
            if n > 1:
                n = n // 2 + 1
            if num_having.get(n):
                num_having[n] += 1
            else:
                num_having[n] = 1
        else:
            num_having["none"] += 1

        # Check if LIMIT clause exists
        if sql["limit"]:
            num_limits["has_limit"] += 1
        else:
            num_limits["no_limit"] += 1

        # Check if INTERSECT clause exists
        if sql["intersect"]:
            num_intersects += 1

        # Check if UNION clause exists
        if sql["union"]:
            num_unions += 1

        # Check if EXCEPT clause exists
        if sql["except"]:
            num_excepts += 1

    # Calculate averages
    avg_selects = {}
    for i in num_selects:
        avg_selects[i] = round(num_selects[i] / sum(num_selects.values()), 3)
    avg_order_by = {}
    for i in order_by_details:
        avg_order_by[i] = round(order_by_details[i] / sum(order_by_details.values()), 3)
    avg_from = {}
    for i in from_details:
        avg_from[i] = round(from_details[i] / sum(from_details.values()), 3)
    avg_where = {}
    for i in where_details:
        avg_where[i] = round(where_details[i] / sum(where_details.values()), 3)
    avg_group_by = {}
    for i in num_column_name_in_group_by:
        avg_group_by[i] = round(
            num_column_name_in_group_by[i] / sum(num_column_name_in_group_by.values()),
            3,
        )
    avg_having = {}
    for i in num_having:
        avg_having[i] = round(num_having[i] / sum(num_having.values()), 3)
    avg_limits = {}
    for i in num_limits:
        avg_limits[i] = round(num_limits[i] / sum(num_limits.values()), 3)
    avg_intersects = round(num_intersects / num_queries, 3)
    avg_unions = round(num_unions / num_queries, 3)
    avg_excepts = round(num_excepts / num_queries, 3)
    # avg_num_column_name_in_group_by = num_column_name_in_group_by / num_queries

    # Print averages
    print()
    print("-" * 20 + "SELECT STATEMENTS" + "-" * 20)
    print()

    print("Number of SELECT statements per query:", num_selects)
    print("Average number of SELECT statements per query:", avg_selects)
    print()
    print("-" * len("-" * 20 + "SELECT STATEMENTS" + "-" * 20))
    print()
    print("-" * 23 + "FROM CLAUSE" + "-" * 23)
    print("FROM details:", from_details)
    print("Average number of FROM clause:", avg_from)
    print()
    print("-" * 23 + "WHERE CLAUSE" + "-" * 23)
    print()
    print("WHERE details:", where_details)
    print("number of where clause", num_where)

    print("Average number of WHERE clauses per query:", avg_where)
    print()
    print("-" * 23 + "GROUP BY CLAUSE" + "-" * 23)
    print("number of column names in GROUP BY:", num_column_name_in_group_by)
    print("Average number of queries with GROUP BY:", avg_group_by)
    print()
    print("-" * 23 + "HAVING CLAUSE" + "-" * 23)
    print("HAVING details:", num_having)
    print("Average number of HAVING clauses per query:", avg_having)
    print()
    print("-" * 23 + "ORDER BY CLAUSES" + "-" * 23)
    print("ORDER BY details:", order_by_details)
    print("Average number of queries with ORDER BY:", avg_order_by)
    print()
    print("-" * 23 + "LIMIT CLAUSES" + "-" * 23)
    print("LIMIT details:", num_limits)
    print("Average number of LIMIT clauses per query:", avg_limits)
    print()
    print("-" * 23 + "Other details:" + "-" * 23)
    print("Average number of INTERSECT clauses per query:", avg_intersects)
    print("Average number of UNION clauses per query:", avg_unions)
    print("Average number of EXCEPT clauses per query:", avg_excepts)
