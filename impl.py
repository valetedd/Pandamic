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
        except ValueError as e:
            print(f"{e}: input argument must be a string")
        except FileNotFoundError as f:
            print("File not found. Try specifying a different path")
        else:
            print("Unsupported format. Only .csv or .json files can be specified")
            return False
            
class ProcessDataUploadHandler(UploadHandler):

    def pushDataToDb(self, json_path: str) -> bool:

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)             
            raw_df = njson_to_df(data) # Custom function to traverse json
            raw_df = raw_df.map(regularize_data) # Regularizing datatypes 
            act_df = add_hash_ids(raw_df) # Adding stable-hash internal IDs

            # Selecting sub-dataframes for each type
            db = self.getDbPathOrURL()
            types = act_df["type"].unique()
            for type in types:
                sdf = act_df[act_df["type"] == type].drop(columns=["type"])
                df_name = f"{type}Data" # table name to use fo upload
                safe_upload(sdf, df_name, db) # uploading them to the selected db
            print("Data succesfully uploaded to database!")
            return True
        except ValueError as e:
            print(f"{e}: input argument must be a string")
            return False
        except FileNotFoundError:
            print("File not found. Try specifying a different path")
            return False

class MetadataUploadHandler(UploadHandler):
    pass

### Tests ###
process = ProcessDataUploadHandler()
process.setDbPathOrUrl("databases/relational.db")
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
    def cleanMetadataHandlers() -> bool:
        pass
    def cleanProcessHandlers() -> bool:
        pass
    def addMetadataHandler(handler: MetadataQueryHandler) -> bool:
        pass
    def addProcessHandler(handler: ProcessDataQueryHandler) -> bool:
        pass
    def getEntityById(id: str):
        pass
    def getAllPeople():
        pass
    def getAllCulturalHeritageObjects():
        pass
    def getAuthorsOfCulturalHeritageObjects(objectId: str):
        pass
    def getCulturalHeritageObjectsAuthoredBy(personalId: str):
        pass
    def getAllActivities():
        pass
    def getActivitiesByResponsibleInstitution(partialName: str):
        pass
    def getActivitiesByResponsiblePerson(partialName: str):
        pass
    def getActivitiesUsingTool(partialName: str):
        pass
    def getActivitiesStartedAfter(date: str):
        pass
    def getActivitiesEndedAfter(date: str):
        pass
    def getAcquisitionByTechnique(partialName: str):
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
