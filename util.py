import json
import pandas as pd
from pprint import pprint
from sqlite3 import connect

def njson_to_df(json_path):
    matrix = [] #  list to store the rows of the dataframe as lists
    techniques = [] # list to store the techniques separately
    # Parsing the json file
    with open(json_path, "r", encoding="utf-8") as f:           
            data = json.load(f) # reading the Json file (list of dictionaries)
    for dict in data: # iterating over the list of dictionaries
          id = dict.pop("object id") 
          for act_type in dict: # iterating over the activity-types dictionaries nested into each dictionary
                row = [act_type, id]
                for key in dict[act_type]: # iterating over the keys of each nested dictionary
                    value = dict[act_type][key]
                    if key == "technique":
                        techniques.append(value)
                    else:
                        row.append(value)
                matrix.append(row)
    raw_df = pd.DataFrame(matrix, columns=["type", "object_id", "responsible_institute", 
                                           "responsible_person", "tool", "start_date", "end_date"])
    # Adding the techniques as a separate column
    tech_series = pd.Series(techniques, dtype="str", name="technique")
    raw_df["object_id"] = raw_df["object_id"].apply(lambda x: int(x))
    raw_df.sort_values(by=["type", "object_id"], inplace=True, ignore_index=True) # sorting df to match the techniques' order
    full_df = pd.concat([raw_df, tech_series], axis=1) 

    return full_df


