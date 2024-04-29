import pandas as pd
from sqlite3 import connect
import json
from util import *
import hashlib

############# ENTITIES ###############

class Activity():
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None):
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
    def __init__(self, institute:str, technique:str, person: str | None, tool: str | None, start: str | None, end: str | None):
        self.technique = technique
        super().__init__(institute, person, tool, start, end)
        
    def getTechnique(self):
        return self.technique
    
class Processing(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None):
        super().__init__(institute, person, tool, start, end)

class Modelling(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None):
        super().__init__(institute, person, tool, start, end)

class Optimising(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None):
        super().__init__(institute, person, tool, start, end)

class Exporting(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None):
        super().__init__(institute, person, tool, start, end)

################## UPLOAD MANAGEMENT ######################
        
class Handler(object):
    def __init__(self):
        self.dbPathOrURL = ""

    def getDbPathOrURL(self):
        return self.dbPathOrURL
    
    def setDbPathOrUrl(self, pathOrURL: str) -> bool:
        try:
            self.dbPathOrURL = pathOrURL
            print("Database location succesfully updated")
            return True
        except ValueError as e:
            print(f"{e}: input argument must be a string")
            return False
        except TypeError:
            print("Please specify a path or URL")
        
class UploadHandler(Handler):

    def pushDataToDb(self, path: str) -> bool:
        try:
            db = self.getDbPathOrURL()
            if ".csv" in path:
                meta = MetadataUploadHandler()
                meta.setDbPathOrUrl(db)
                meta.pushDataToDb(path)
                return True
            elif ".json" in path:
                pro = ProcessDataUploadHandler()
                pro.setDbPathOrUrl(db)
                pro.pushDataToDb(path)
                return True
            else:
                print("Unsupported format. Only .csv or .json files can be specified")
                return False
        except ValueError as e:
            print(f"{e}: input argument must be a string")
            return False
        except FileNotFoundError:
            print("File not found. Try specifying a different path")
            return False
        except TypeError:
            print("Please specify a path or URL")
            return False
            
class ProcessDataUploadHandler(UploadHandler):

    def pushDataToDb(self, json_path: str) -> bool:

        try:
            # Loading data into a DataFrame
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)             
            act_df = njson_to_df(data) # Converting json to Dataframe in accordance to the data model
            act_df = act_df.map(regularize_data) # Regularizing datatypes 
            
            # Adding column of stable hashes as internal IDs
            int_ids = generate_hash_ids(act_df, prefix="act-")
            act_df.insert(0, "internal_id",int_ids) 

            # Uploading data to the selected db       
            db = self.getDbPathOrURL()        
            types = act_df["type"].unique()
            with connect(db) as con:
                for type in types:
                    sdf = act_df[act_df["type"] == type].drop(columns=["type"]) # dividing data by type in sub-dataframes
                    df_name = f"{type}Data" # table name to use fo upload
                    sdf.to_sql(df_name, con, if_exists="append", index=False)
                    print(f"{df_name} succesfully uploaded")
                    cur = con.cursor()
                    cur.execute(f"""DELETE FROM {df_name} WHERE rowid NOT IN 
                                    (SELECT MIN(rowid) FROM {df_name} 
                                    GROUP BY internal_id);""") 
            return True
        
        except ValueError as e:
            print(f"{e}: input argument must be a string")
            return False
        except FileNotFoundError:
            print("File not found. Try specifying a different path")
            return False
        except TypeError:
            print("Please specify the path of the JSON file you want to upload")
            return False
        except Exception as e:
            print(f"{e}")
            return False

class MetadataUploadHandler(UploadHandler):
    pass

### Tests ###
process = ProcessDataUploadHandler()
process.setDbPathOrUrl("stuff/test.db")
process.pushDataToDb("stuff/test.json")
# obj = UploadHandler()
# obj.setDbPathOrUrl("databases/relational.db")
# print(obj.pushDataToDb("data/process.json"))



############ QUERY MANAGEMENT #################
    
class QueryHandler(Handler):
    
    def getById(self, id: str) -> pd.DataFrame:
        pass

class ProcessDataQueryHandler(QueryHandler):

    def getAllActivities(self) -> pd.DataFrame:
        pass
    
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> pd.DataFrame:
        pass

    def getActivitiesByResponsiblePerson(self, partialName: str) -> pd.DataFrame:
        pass
        
    def getActivitiesUsingTool(self, partialName: str) -> pd.DataFrame:
        pass
        
    def getActivitiesStartedAfter(self, date: str) -> pd.DataFrame:
        pass
        
    def getActivitiesEndedBefore(self, date: str) -> pd.DataFrame:
        pass
        
    def getAcquisitionsByTechnique(partialName: str) -> pd.DataFrame:
        pass

class MetadataQueryHandler(QueryHandler):

    def getAllPeople() -> pd.DataFrame:
        pass
    def getAllCulturalHeritageObjects() -> pd.DataFrame:
        pass
    def getAuthorsOfCulturalHeritageObject(objectId: str) -> pd.DataFrame:
        pass

    def getCulturalHeritageObjectsAuthoredBy(authorId: str) -> pd.DataFrame:
        pass

### Test
# obj = ProcessDataQueryHandler()
# print(obj.getAllActivities())

############## MASHUP #################

class BasicMashup:
    def __init__(self):
        self.metadataQuery = list[MetadataQueryHandler]
        self.processdataQuery = list[ProcessDataQueryHandler]

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery.clear()
        print("MetaData handler list succesfully cleared")
        return True
    
    def cleanProcessHandlers(self) -> bool:
        self.processdataQuery.clear()
        print("ProcessData handler list succesfully cleared")
        return True
    
    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        try:
            self.metadataQuery.append(handler)
            print("MetaData handler succesfully added to the handler list")
            return True
        except TypeError:
            print("Please specify a handler to be added")

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        try:
            self.processdataQuery.append(handler)
            print("ProcessData handler succesfully added to the handler list")
            return True
        except TypeError:
            print("Please specify a handler to be added")

    def getEntityById(id: str):
        pass
    def getAllPeople() -> list[IdentifiableEntity]:
        pass
    def getAllCulturalHeritageObjects() -> list[CulturalHeritageObjects]:
        pass
    def getAuthorsOfCulturalHeritageObjects(objectId: str) -> list[Person]:
        pass
    def getCulturalHeritageObjectsAuthoredBy(personalId: str) -> list[CulturalHeritageObjects]:
        pass
    def getAllActivities(self) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            for row, _ in handler.iterrows():
                curr_type = row["type"] 
                if curr_type == "acquisition":
                    obj = Acquisition(institute=row["responsible institute"], technique=row["technique"], 
                                person=row["responsible person"], tool=row["tool"], start=row["start date"], 
                                end=row["end date"])
                elif curr_type == "processing":
                    obj = Processing(institute=row["responsible institute"], person=row["responsible person"], 
                                     tool=row["tool"], start=row["start date"], end=row["end date"])
                elif curr_type == "modelling":
                    obj = Modelling(institute=row["responsible institute"], person=row["responsible person"], 
                                     tool=row["tool"], start=row["start date"], end=row["end date"])
                elif curr_type == "optimising":
                    obj = Optimising(institute=row["responsible institute"], person=row["responsible person"], 
                                     tool=row["tool"], start=row["start date"], end=row["end date"])
                else:
                    obj = Exporting(institute=row["responsible institute"], person=row["responsible person"], 
                                     tool=row["tool"], start=row["start date"], end=row["end date"])
                result.append(obj)
        
        self.cleanProcessHandlers()
        return result

    def getActivitiesByResponsibleInstitution(partialName: str) -> list[Activity]:
        pass
    def getActivitiesByResponsiblePerson(partialName: str) -> list[Activity]:
        pass
    def getActivitiesUsingTool(partialName: str) -> list[Activity]:
        pass
    def getActivitiesStartedAfter(date: str) -> list[Activity]:
        pass
    def getActivitiesEndedAfter(date: str) -> list[Activity]:
        pass
    def getAcquisitionByTechnique(partialName: str) -> list[Acquisition]:
        pass

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(personId: str):
        pass
    def getObjectsHandledByResponsiblePerson(partialName: str):
        pass
    def getObjectsHandledByResponsibleInstitution(partialName: str):
        pass
    def getAuthorsOfObjectsAcquiredInTimeFrame(start: str, end: str):
        pass


