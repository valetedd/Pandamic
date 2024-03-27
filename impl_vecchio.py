import pandas as pd
from sqlite3 import connect
import json

class Activity():
    def __init__(self, institute, person = None, tool = None, start = None, end = None):
        self.institute = institute
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end
    
    def getResponsibleInstitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        return self.person
    
    def getTool(self):
        return self.tool
    
    def getStartDate(self):
        return self.start
    
    def getEndDate(self):
        return self.end
    
    def refersTo(self):
        pass

class Acquisition(Activity):
    def __init__(self, institute, person, tool, start, end, technique):
        super().__init__(institute, person, tool, start, end)
        self.technique = technique
    
    def getTechnique(self):
        return self.technique
    
class Processing(Activity):
    def __init__(self, institute, person, tool, start, end):
        super().__init__(institute, person, tool, start, end)

class Modelling(Activity):
    def __init__(self, institute, person, tool, start, end):
        super().__init__(institute, person, tool, start, end)

class Optimising(Activity):
    def __init__(self, institute, person, tool, start, end):
        super().__init__(institute, person, tool, start, end)

class Exporting(Activity):
    def __init__(self, institute, person, tool, start, end):
        super().__init__(institute, person, tool, start, end)

#####################################################################
        
class Handler(object):
    def __init__(self):
        self.dbPathOrURL = ""

    def getDbPathOrURL(self):
        return self.dbPathOrURL
    
    def setDbPathOrURL(self, pathOrURL):
        if type(pathOrURL) is str:
            self.dbPathOrURL = pathOrURL
            print("Database location succesfully updated")
            return True
        else:
            print("Input argument must be a string")
            False
        
class UploadHandler(Handler):

    def pushDataToDb(self, path: str):
        if ".csv" in path:
            MetadataUploadHandler(path)
            print("Data succesfully uploaded to database!")
            return True
        elif ".json" in path:
            ProcessUploadHandler(path)
            print("Data succesfully uploaded to database!")
            return True
        else:
            print("Unsupported format. Only .csv or .json files can be specified")
            return False
            
class ProcessUploadHandler(UploadHandler):
    
    def __init__(self, json_path):
        self.db_loc = self.getDbPathOrURL()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.json_df = pd.json_normalize(data, sep=": ")
        self.json_df = self.json_df.map(lambda x: ", ".join(x) if type(x) == list else x)
        with connect(self.db_loc) as con:
            self.json_df.to_sql("ProcessData", con, if_exists="replace", index=False)
        

class MetadataUploadHandler(UploadHandler):
    def __init__(self, csv_path):
        pass

Handler().setDbPathOrURL("data/processData.db")
UploadHandler().pushDataToDb("data/process.json")

