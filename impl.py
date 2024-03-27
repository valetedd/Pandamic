import pandas as pd
from sqlite3 import connect
import json

############# ENTITIES ###############

class Activity():
    def __init__(self, institute, person=None, tool=None, start=None, end=None):
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
        self.technique = technique
        super().__init__(institute, person, tool, start, end)
        
    
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

################## UPLOAD MANAGEMENT ######################
        
class Handler(object):
    def __init__(self):
        self.dbPathOrURL = ""

    def getDbPathOrURL(self):
        return self.dbPathOrURL
    
    def setDbPathOrUrl(self, pathOrURL):
        if isinstance(pathOrURL, str):
            self.dbPathOrURL = pathOrURL
            print(self.dbPathOrURL)
            print("Database location succesfully updated")
            return True
        else:
            print("Input argument must be a string")
            return False
        
class UploadHandler(Handler):

    def pushDataToDb(self, path: str):
        db = self.getDbPathOrURL()
        if ".csv" in path:
            meta = MetadataUploadHandler()
            meta.setDbPathOrUrl(db)
            meta.pushDataToDb(path)
            print("Data succesfully uploaded to database!")
            return True
        elif ".json" in path:
            pro = ProcessDataUploadHandler()
            pro.setDbPathOrUrl(db)
            pro.pushDataToDb(path)
            print("Data succesfully uploaded to database!")
            return True
        else:
            print("Unsupported format. Only .csv or .json files can be specified")
            return False
            
class ProcessDataUploadHandler(UploadHandler):

    def pushDataToDb(self, json_path):
        # Reading the JSON file
        with open(json_path, "r", encoding="utf-8") as f:           
            data = json.load(f)

        # Preprocessing the file, by flattening the JSON's nested structture into a dataframe
        # and converting the list-type values in comma separated strings:
        process_df = pd.json_normalize(data, sep=": ")
        process_df = process_df.map(lambda x: ", ".join(x) if type(x) == list else x)

        # Creating new sub-dataframes corresponding to each type of activity in the data model and adding 
        # the object id column:
        id_column = process_df["object id"]

        acq_sdf = process_df.loc[:, "object id":"acquisition: end date"]

        pro_sdf = process_df.loc[:, "processing: responsible institute":"processing: end date"]
        pro_sdf.insert(0, "object id", id_column)

        mod_sdf = process_df.loc[:, "modelling: responsible institute":"modelling: end date"]
        mod_sdf.insert(0, "object id", id_column)

        opt_sdf = process_df.loc[:, "optimising: responsible institute":"optimising: end date"]
        opt_sdf.insert(0, "object id", id_column)

        exp_sdf = process_df.loc[:, "exporting: responsible institute":"exporting: end date"]
        exp_sdf.insert(0, "object id", id_column)

    # Uploading the resulting dataframes to the relational database:
        db = self.getDbPathOrURL()
        print(db)
        with connect(db) as con:
            acq_sdf.to_sql("AcquisitionData", con, if_exists="replace", index=False)
            pro_sdf.to_sql("ProcessingData", con, if_exists="replace", index=False)
            mod_sdf.to_sql("ModellingData", con, if_exists="replace", index=False)
            opt_sdf.to_sql("OptimisingData", con, if_exists="replace", index=False)
            exp_sdf.to_sql("ExportingData", con, if_exists="replace", index=False)
        

class MetadataUploadHandler(UploadHandler):
    pass

### Tests ###
# process = ProcessDataUploadHandler()
# process.setDbPathOrUrl("databases/relational.db")
# process.pushDataToDb("data/process.json")
# obj = UploadHandler()
# obj.setDbPathOrUrl("databases/relational.db")
# print(obj.pushDataToDb("data/process.json"))



############ QUERY MANAGEMENT #################
    
class QueryHandler(Handler):
    
    def getById(self, id):
        pass

class ProcessDataQueryHandler(QueryHandler):

    def getAllActivities(self):
        pass
    
    def getActivitiesByResponsibleInstitution(self, partialName):
        pass

    def getActivitiesByResponsiblePerson(self, partialName):
        pass
    def getActivitiesUsingTool(self, partialName):
        pass
    def getActivitiesStartedAfter(self, date: str):
        pass
    def getActivitiesEndedBefore(self, date: str):
        pass
    def getAcquisitionsByTechnique(partialName):
        pass

class MetadataQueryHandler(QueryHandler):

    def getAllPeople():
        pass
    def getAllCulturalHeritageObjects():
        pass
    def getAuthorsOfCulturalHeritageObject(objectId):
        pass

    def getCulturalHeritageObjectsAuthoredBy(authorId: str):
        pass

############## MASHUP #################

class BasicMashup:
    def __init__(self, mq=None, pq=None):
        self.metadataQuery = list(mq)
        self.processdataQuery = list(pq)
    def cleanMetadataHandlers():
        pass
    def cleanProcessHandlers():
        pass
    def addMetadataHandler(handler):
        pass
    def addProcessHandler(handler):
        pass
    def getEntityById(id):
        pass
    def getAllPeople():
        pass
    def getAllCulturalHeritageObjects():
        pass
    def getAuthorsOfCulturalHeritageObjects(objectId):
        pass
    def getCulturalHeritageObjectsAuthoredBy(personalId):
        pass
    def getAllActivities():
        pass
    def getActivitiesByResponsibleInstitution(partialName):
        pass
    def getActivitiesByResponsiblePerson(partialName):
        pass
    def getActivitiesUsingTool(partialName):
        pass
    def getActivitiesStartedAfter(date):
        pass
    def getActivitiesEndedAfter(date):
        pass
    def getAcquisitionByTechnique(partialName):
        pass

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(personId):
        pass
    def getObjectsHandledByResponsiblePerson(partialName):
        pass
    def getObjectsHandledByResponsibleInstitution(partialName):
        pass
    def getAuthorsOfObjectsAcquiredInTimeFrame(start, end):
        pass



