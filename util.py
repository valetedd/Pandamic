import pandas as pd
import hashlib
from collections import deque, defaultdict

def njson_to_df(json_data : list[dict],
                attributes: tuple, 
                types: tuple,
                id_key: str = None) -> pd.DataFrame:
                
    data_dict = defaultdict(list) # Dictionary with list values to rearrange the data contained in the JSON

    # Breadth-first approach to traverse the JSON
    to_visit = deque(json_data)   
    while to_visit: # Iterating over the the queue of dictionaries to be visited by popping the first item
        curr_dict = to_visit.popleft() 
        id_val = curr_dict.pop(id_key) if id_key in curr_dict else None # Getting the id value in the current dict, if present
        attr_check = False
        for k, v in curr_dict.items(): # Iterating over the key-value pairs of the current dict 
            if isinstance(v, dict): # Handling nested dictionaries
                to_visit.append(v) 

             # Dealing with other data
            else:
                data_dict[k].append(v)

            if k in types:
                data_dict["type"].append(k)
                if id_val:
                    data_dict[id_key].append(id_val)

            elif k in attributes and not attr_check:
                attr_check = True
            
        # Handling missing keys
        if attr_check:
            dict_keys = curr_dict.keys()
            missing_keys = set(attributes).difference(dict_keys)
            if missing_keys and len(missing_keys) < len(dict_keys):
                for key in missing_keys:
                    data_dict[key].append(None)

    # Constructing DataFrame from data_dict and making it prettier
    df = pd.DataFrame(data_dict)
    df.columns = [col.replace(" ", "_") for col in df.columns]
    df = df.loc[: , ["type", "responsible_institute", "responsible_person","tool", 
                    "start_date", "end_date", "technique", "object_id"]]
    return df

def regularize_data(x) -> str:
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
            print(f"--- {func} RETURNED {result} ---")
            if result and isinstance(result, object):
                print(f"{result.__dict__}\n\n")
            else:
                print("\n\n")
        return result
    return wrapper