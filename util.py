import pandas as pd
import hashlib
from typing import Any, Iterable

def njson_to_df(json_data : list[dict]) -> pd.DataFrame:
    try:
        df_dict = {"type":[], "responsible institute":[], "responsible person":[],"tool":[], # dictionary of expected labels 
                    "start date":[], "end date":[], "technique":[], "object id":[]}          # based on the data model's specifications
        act_attrs = {"responsible institute", "responsible person", "tool", "start date", "end date"}
        # Traversing the json file
        for item in json_data: # iterating over the list of dictionaries
            id = item.pop("object id")
            for act_type in item: # iterating over the activity-type dictionaries
                df_dict["object id"].append((id))
                df_dict["type"].append(act_type)
                if act_type != "acquisition":
                    df_dict["technique"].append(None)
                for attribute in (attributes := item[act_type]): # iterating over the attribute-keys of each nested dictionary
                    value = item[act_type][attribute]
                    df_dict[attribute].append(value)
                if len(attributes) < len(act_attrs): # handling missing key cases
                    missing_attrs = act_attrs.difference(set(attributes))
                    for attr in missing_attrs:
                        df_dict[attr].append("")
        df = pd.DataFrame(df_dict)
        df.columns = [col.replace(" ", "_") for col in df.columns]
        return df
    except KeyError as e:
        print(f"{e}: json data is not well-formed")
        return pd.DataFrame()

def regularize_data(x : Any) -> str:
    if isinstance(x, (list, set, tuple)):
        return ", ".join(x)
    elif isinstance(x, dict):
        values = [str(x[key]) for key in x.keys()]
        return ", ".join(values)
    elif isinstance(x, int):
        return str(x)
    else:
        return x
    
def hash_ids_for_df (df: pd.DataFrame, prefix: str) -> list[str]:
    int_ids = []
    for row in df.itertuples(index=False):
        algorithm = hashlib.sha256()
        algorithm.update(str(row).encode("utf-8"))
        curr_hash = algorithm.hexdigest()
        int_ids.append(prefix + curr_hash[:7]) 
    return int_ids

def print_attributes(func):
    def wrapper(*args):
        result = func(*args)
        counter = 0
        if isinstance(result, list) and result:
            print(f"--- ATTRIBUTES OF OBJECTS RETURNED BY {func} ---\n\n")
            for obj in result:
                print(f"""Index {counter}:\n{type(obj)}; \n{list(obj.__dict__.values())}\n\n""")
                counter += 1
        else:
            print(f"--- {func} RETURNED {result} ---\n\n")
        return result
    return wrapper
