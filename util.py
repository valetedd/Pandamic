import pandas as pd
import hashlib
from typing import Any, Iterable
from collections import deque

def njson_to_df(json_data : list[dict], types: tuple = ("acquisition", "processing", "modelling", "optimising", "exporting"), sparse_data: dict = {"technique" : ["acquisition"]}) -> pd.DataFrame:
    try:
        data_dict = {} # Dicitionary to store the data contained in the JSON

        # Breadth-first approach to traversing the JSON
        to_visit = deque(json_data)
        while to_visit: # Iterating over the lenght of the queue of dictionaries to be visited by popping the first item
            curr_item = to_visit.popleft()
            for k, v in curr_item.items(): # Iterating over the key-value pairs of the current dict
                # Handling nested dictionaries
                if isinstance(v, dict):
                    to_visit.append(v) # appending nested dictionaries to the end of queue
                    if k in types and "type" not in data_dict:    
                        data_dict["type"] = [k]  
                    else:
                        data_dict["type"].append(k)

                # Dealing with other data
                else: 
                    if not k in data_dict:
                        data_dict[k] = [v] 
                    else:
                        data_dict[k].append(v)

                    # Repeating Id value for each type they refer to
                    if isinstance(v, str) and v.isdigit():
                       data_dict[k].extend([v for _ in range(len(types)-1)])

                    # Handling sparse data 
                    elif sparse_data and k in sparse_data:
                        sparse_len = len(sparse_data[k])
                        n = len(types) - sparse_len
                        data_dict[k].extend([None for _ in range(n)])

        # Constructing DataFrame from data_dict and making it prettier
        df = pd.DataFrame(data_dict)
        df.columns = [col.replace(" ", "_") for col in df.columns]
        df = df.loc[: , ["type", "responsible_institute", "responsible_person","tool", 
                        "start_date", "end_date", "technique", "object_id"]]
        return df
    
    except KeyError as e:
        print(f"{e}: json data is not well-formed")
        return pd.DataFrame()
    except TypeError as t:
        print(t)
        return pd.DataFrame()

def regularize_data(x : Any) -> str:
    if isinstance(x, (list, set, tuple)):
        return ", ".join(x)
    elif isinstance(x, dict):
        values = [str(val) for val in x.values()]
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
