import glob
import pandas as pd
import csv
import os
from parser_sql.parse_sql_one import get_schema, Schema, get_sql, get_schema_from_json
if __name__ == "__main__":
    folder_path = "data/synthetic-queries/"
    files = glob.glob(folder_path + "/*.csv")
    print(files)
    for file in files:
        db_name = file.split("/")[-1].split(".")[0].split("_res")[0]
        print(file)
        df = pd.read_csv(file)
        print(df.head())
        flag = False
        db2 = db_name
        print(db2)

        db2 = os.path.join("test-suite-sql-eval-master/database/", db2, db2 + ".sqlite")
        schema2 = Schema(get_schema(db2))
        print(schema2.schema)
        print("++++++++++++++++++++++=")
        for i in range(len(df)):
            partial_query = df.iloc[i]["query"]
            print("query:",partial_query)
            try:
                g_sql = get_sql(schema2, partial_query)
                print(g_sql)
                flag = True
                print("Parsed Successfully")
            except Exception as e:
                print("Error in Parsing:", e.args)

        
        
    