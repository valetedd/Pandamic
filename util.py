import pandas as pd
import json
import hashlib
from sqlite3 import connect
def njson_to_df(json_data, expected_attr={"responsible institute", "responsible person", "tool","start date", "end date"}):
    
    df_dict = {"type":[], "responsible institute":[], "responsible person":[],"tool":[], # dictionary of expected labels 
                "start date":[], "end date":[], "technique":[], "object id":[]}          # based on the data model's specifications
    # Traversing the json file
    for dict in json_data: # iterating over the list of dictionaries
        id = dict.pop("object id")
        for act_type in dict: # iterating over the activity-type dictionaries nested into each dictionary
            df_dict["object id"].append(id)
            df_dict["type"].append(act_type)
            if act_type != "acquisition":
                df_dict["technique"].append("")
            for attribute in (attributes := dict[act_type]): # iterating over the attribute-keys of each nested dictionary
                value = dict[act_type][attribute]
                df_dict[attribute].append(value)
            if len(attributes) < len(expected_attr): # handling missing keys case
                missing_attrs = expected_attr.difference(set(attributes))
                for attr in missing_attrs:
                    df_dict[attr].append("")
    df = pd.DataFrame(df_dict)
    df.columns = [col.replace(" ", "_") for col in df.columns]
    return df

def regularize_data(x):
    if isinstance(x, (list, set, tuple)):
        return ", ".join(x)
    if isinstance(x, dict):
        values = [x[key] for key in x.keys()]
        return ", ".join(values)
    else:
        return x
def add_hash_ids(df: pd.DataFrame):
    int_ids = []
    for row in df.itertuples(index=False):
        algorithm = hashlib.sha256()
        algorithm.update(str(row).encode("utf-8"))
        curr_hash = algorithm.hexdigest()
        int_ids.append("act-" + curr_hash[:7]) 
    df.insert(0, "internal_ids",int_ids)
    return df

def safe_upload(df: pd.DataFrame, name: str, db: str):
    with connect(db) as con:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';") # getting existing table names in db
        zipped_names = cur.fetchall()
        table_names = [table[0] for table in zipped_names if zipped_names]
        if name in table_names:
            print(f"Updating table({name})")
            df.to_sql("temp", con, if_exists="replace", index=False)
            cur.execute(f"""CREATE TABLE new AS SELECT * FROM {name} UNION SELECT * FROM temp;""")
            cur.execute(f"""DROP TABLE temp;""")
            cur.execute(f"""DROP TABLE {name};""")
            cur.execute(f"""ALTER TABLE new RENAME TO {name}""")
        else:
            df.to_sql(name, con, if_exists="replace", index=False)
        return
# with open("data/process.json", "r", encoding="utf-8") as f:
#     data = json.load(f)
# #print(njson_to_df(data))

      