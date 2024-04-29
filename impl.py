import pandas as pd
from sqlite3 import connect
import json
from util import *
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw

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

class MetadataUploadHandler(): # (i.UploadHandler):
    
    def pushDataToDb(self, path: str) -> bool:
        blzgrph = SPARQLUpdateStore()
        endpoint = "http://10.201.16.20:9999/blazegraph/sparql" # if you try this, remember to update the endpoint depending on the one set when running blzgraph

        def check_yoself_befo_yo_shrek_yoself(subj, pred, obj):
            if "http" not in obj:
                obj = '"'+obj+'"'
            else:
                obj = "<"+obj+">"
            request = sw.SPARQLWrapper(endpoint)
            base_query = f"ASK {{ <{subj}> <{pred}> {obj} . }} "
            request.setReturnFormat(sw.JSON)
            request.setQuery(base_query)
            result = request.query().convert()
            return result['boolean']
            


        md_Series = pd.read_csv(path, keep_default_na=False, dtype={
            "Id":"string","Type":"string","Title":"string","Date":"string","Author":"string","Owner":"string","Place":"string"
        })
        md_Series= md_Series.rename(columns={"Id":"identifier","Type":"type","Title":"name","Date":"datePublished","Author":"author","Owner":"maintainer","Place":"spatial"})
        
        PDM = rdf.Namespace("http://ourwebsite/") #our base URI
        graph_to_upload=rdf.Graph() # creating the graph that i will upload
        graph_to_upload.bind("pdm", PDM)
        
        dict_of_obj_uri=dict()      
        our_obj = list(md_Series["type"].unique())
        our_obj.extend(list(md_Series["maintainer"].unique())) 
        our_obj.extend(list(md_Series["spatial"].unique())) #creating a list with all the unique values from the DF
        for item in our_obj:
            url_friendly_name=item.replace(" ", "_")
            dict_of_obj_uri[item]=PDM+url_friendly_name # <-- adding to the dictionary each item with its own
            # generated URI.

        authors_viaf=dict() # I had to take them from VIAF or ULAN
        
        for idx, row in md_Series.iterrows():
            # Internal_ID="ID"+str(idx)
            subj = PDM + row["identifier"] # generating a specific URI for each item
            
            for pred, obj in row.items():
                if pred=="author":
                    obj_list=obj.split(" (")
                    name=obj_list[0] #assigning the actual name to this variable
                    if len(name)==0:
                        pass
                    else:
                        author_ID=obj_list[1]
                        author_ID=author_ID[0:-1]
                        if "VIAF" in author_ID:                        # depending on the type of ID we get a differet URI
                            author_url = rdf.URIRef("http://viaf.org/" + author_ID.replace(":","/").lower())
                        elif "ULAN" in author_ID:
                            author_url = rdf.URIRef("http://vocab.getty.edu/page/" + author_ID.replace(":","/").lower())
                        else:
                            author_url = rdf.URIRef(PDM + author_ID.replace(":","/").lower())
                        if name not in authors_viaf: # adding the author to the dictionary if they're not present
                            authors_viaf[name] = author_ID
                        # aut = authors_viaf[name]
                        
                        if not(check_yoself_befo_yo_shrek_yoself(author_url, SDO.identifier, author_ID)):
                            graph_to_upload.add((rdf.URIRef(author_url),SDO.identifier,rdf.Literal(author_ID))) # one triple for each author: author's URI-its ID

                        if not(check_yoself_befo_yo_shrek_yoself(subj, SDO.author, author_url)):
                            graph_to_upload.add((rdf.URIRef(subj),SDO.author,rdf.URIRef(author_url)))          # URI of the book-URI of the author

                        if not(check_yoself_befo_yo_shrek_yoself(author_url, SDO.givenName, name)):
                            graph_to_upload.add((rdf.URIRef(author_url),SDO.givenName,rdf.Literal(name)))  # URI of the author - its name
                else:
                    if pred == "type":
                        if not(check_yoself_befo_yo_shrek_yoself(subj, RDF.type, dict_of_obj_uri[obj])):
                            graph_to_upload.add((rdf.URIRef(subj),RDF.type,rdf.URIRef(dict_of_obj_uri[obj]))) # 2 triples, book's URI - type's URI,
                        
                        if not(check_yoself_befo_yo_shrek_yoself(dict_of_obj_uri[obj], RDFS.label, obj)):
                            graph_to_upload.add((rdf.URIRef(dict_of_obj_uri[obj]),RDFS.label,rdf.Literal(obj)))  # type's URI - type's name. Since
                        # graphs don't allow redundancy, only one of the second triple will be added for each type.

                    elif pred == "maintainer" or pred=="spatial":
                        if not(check_yoself_befo_yo_shrek_yoself(subj, SDO+pred, dict_of_obj_uri[obj])):
                            graph_to_upload.add((rdf.URIRef(subj), rdf.URIRef(SDO+pred), rdf.URIRef(dict_of_obj_uri[obj]))) #Same concept for the other 2, but
                        
                        if not(check_yoself_befo_yo_shrek_yoself(dict_of_obj_uri[obj], RDFS.label, obj)):
                            graph_to_upload.add((rdf.URIRef(dict_of_obj_uri[obj]), RDFS.label, rdf.Literal(obj)))  # they use SDO instead of RDF
                     
                    else:
                        if not(check_yoself_befo_yo_shrek_yoself(subj, SDO+pred, obj)):
                            graph_to_upload.add((rdf.URIRef(subj),rdf.URIRef(SDO+pred),rdf.Literal(obj)))
        
        blzgrph.open((endpoint,endpoint))
        for sent in graph_to_upload.triples((None,None,None)):
            blzgrph.add(sent)
        blzgrph.close()

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
        print("ProcessData handlers-list succesfully cleared")
        return True
    
    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        try:
            self.metadataQuery.append(handler)
            print("Handler succesfully added to the MetaData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        try:
            self.processdataQuery.append(handler)
            print("Handler succesfully added to the ProcessData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False

    def getEntityById(self, id: str):
        pass
    def getAllPeople(self, ) -> list[IdentifiableEntity]:
        pass
    def getAllCulturalHeritageObjects(self, ) -> list[CulturalHeritageObjects]:
        pass
    def getAuthorsOfCulturalHeritageObjects(self, objectId: str) -> list[Person]:
        pass
    def getCulturalHeritageObjectsAuthoredBy(self, personalId: str) -> list[CulturalHeritageObjects]:
        pass
    def getAllActivities(self) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getAllActivities()
            for _, row in query_df.iterrows():
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

    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getActivitiesByResponsibleInstitution(partialName)
            for _, row in query_df.iterrows():
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
    
    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getActivitiesByResponsiblePerson(partialName)
            for _, row in query_df.iterrows():
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
    
    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getActivitiesUsingTool(partialName)
            for _, row in query_df.iterrows():
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
    
    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getActivitiesStartedAfter(date)
            for _, row in query_df.iterrows():
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
    
    def getActivitiesEndedAfter(self, date: str) -> list[Activity]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getActivitiesEndedAfter(date)
            for _, row in query_df.iterrows():
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
    
    def getAcquisitionByTechnique(self, partialName: str) -> list[Acquisition]:
        result = []
        for handler in self.processdataQuery:
            query_df = handler.getAcquisitionByTechnique(partialName)
            for _, row in query_df.iterrows():
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

class AdvancedMashup(BasicMashup):
    def getActivitiesOnObjectsAuthoredBy(self, personId: str):
        pass
    def getObjectsHandledByResponsiblePerson(self, partialName: str):
        pass
    def getObjectsHandledByResponsibleInstitution(self, partialName: str):
        pass
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str):
        pass


