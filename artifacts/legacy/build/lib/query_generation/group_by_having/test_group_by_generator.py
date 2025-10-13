from group_by_generator import complete_with_group_by_clause


def test_complete_with_group_by_clause():
    """
    Test the complete_with_group_by_clause function.
    """
    # Test case 1
    query = "FROM farm WHERE Sheep_and_Goats >= Total_Cattle"
    attributes = {
        "number": [
            "Farm_ID",
            "Year",
            "Total_Horses",
            "Working_Horses",
            "Total_Cattle",
            "Oxen",
            "Bulls",
            "Cows",
            "Pigs",
            "Sheep_and_Goats",
        ],
        "text": [],
    }
    unique_tables = ["farm"]
    primary_keys = {"farm": "Farm_ID"}
    number_of_columns = 1

    result = complete_with_group_by_clause(
        query, attributes, unique_tables, primary_keys, number_of_columns
    )
    assert result[0][-1][0] == result[0][0].split("GROUP BY ")[1]


test_complete_with_group_by_clause()
