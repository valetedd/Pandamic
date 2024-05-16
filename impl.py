import pandas as pd
from sqlite3 import connect, OperationalError
import json
from util import *
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw
from datetime import datetime

############# ENTITIES ###############

class IdentifiableEntity():
    def __init__(self, id:str):
        self.id = id
    
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, id:str, name:str):
        super().__init__(id)
        self.name = name

    def getName(self):
        return self.name
    
class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|str|None):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.authors = hasAuthor
    
    def getTitle(self):
        return self.title
    def getDate(self):
        return self.date
    def getOwner(self):
        return self.owner
    def getPlace(self):
        return self.place
    def getAuthors(self):
        return self.authors

class NauticalChart(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class ManuscriptPlate(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class ManuscriptVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class PrintedVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class PrintedMaterial(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Herbarium(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Specimen(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Painting(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Model(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)
        
class Map(CulturalHeritageObject):
    def __init__(self, id:str, title: str, date: str|None, owner: str, place: str, hasAuthor: list|None):
        super().__init__(id, title, date, owner, place, hasAuthor)

class Activity():
    def __init__(self, institute:str, person: str|None, tool: str|None, start: str|None, end: str|None, refersTo: CulturalHeritageObject):
        self.institute = institute
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end
        self.refersToObj = refersTo
    
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> str | None:
        return self.person
    
    def getTool(self) -> set[str]:
        if self.tool is None:
            return set("")
        else:
            return set(self.tool.split(", "))
    
    def getStartDate(self) -> str | None:
        return self.start
    
    def getEndDate(self) -> str | None:
        return self.end
    
    def refersTo(self) -> CulturalHeritageObject:
        return self.refersToObj
    
class Acquisition(Activity):
    def __init__(self, institute:str, technique:str, person: str | None, tool: str | None, start: str | None, end: str | None, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.technique = technique
        
    def getTechnique(self):
        return self.technique
    
class Processing(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Modelling(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Optimising(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Exporting(Activity):
    def __init__(self, institute:str, person: str | None, tool: str | None, start: str | None, end: str | None, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

################## UPLOAD MANAGEMENT ######################
        
class Handler(object):                                   

    dbPathOrURL = ""

    def getDbPathOrURL(self):
        return self.dbPathOrURL
    
    def setDbPathOrUrl(self, pathOrURL: str) -> bool:
        try:    
            if isinstance(pathOrURL, str):
                self.dbPathOrURL = pathOrURL
                print(f"Database location for {self} succesfully set")
                return True
            else:
                raise ValueError
        except ValueError as v:
            print(f"{v}: input has to be a string")
            return False
        except TypeError:
            print("Please specify a path or URL")
            return False
            
### TESTS ###
# h = Handler()
# h.setDbPathOrUrl("mamma")
# print(h.getDbPathOrURL())
#############
        
class UploadHandler(Handler):

    def pushDataToDb(self, path: str) -> bool:
        try:
            db = self.getDbPathOrURL()
            if path.endswith(".csv"):
                meta = MetadataUploadHandler()
                meta.setDbPathOrUrl(db)
                meta.pushDataToDb(path)
                return True
            elif path.endswith(".json"):
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
        except OperationalError:
            print("Upload to db failed. Try resetting the db path or check for inconsistencies in your data")
            return False
        except TypeError:
            print("Please specify a path or URL")
            return False
        except Exception as e:
            print(f"{e}")
            return False
            
class ProcessDataUploadHandler(UploadHandler):

    def pushDataToDb(self, json_path: str) -> bool:

        try:
            # Loading data into a DataFrame
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)             
            act_df = njson_to_df(data) # Converting json to Dataframe according to the data model
            act_df = act_df.map(regularize_data) # Regularizing datatypes 
            
            # Adding column of stable hashes as internal IDs
            int_ids = hash_ids_for_df(act_df, prefix="act-")
            act_df.insert(0, "internal_id",int_ids) 

            # Uploading data to the selected db       
            db = self.getDbPathOrURL()        
            types = act_df["type"].unique()
            with connect(db) as con:
                for t in types:
                    sdf = act_df[act_df["type"] == t] # dividing data in sub-dataframes by type   
                    df_name = f"{t}Data" # table name to use in the db
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
        except OperationalError:
            print("Upload to db failed. Try resetting the db path or check for inconsistencies in your data")
            return False
        except TypeError:
            print("Please specify the path of the JSON file you want to upload")
            return False
        except Exception as e:
            print(f"{e}")
            return False
### Tests ###
# process = ProcessDataUploadHandler()
# process.setDbPathOrUrl("databases/relational.db")
# process.pushDataToDb("data/process.json")
# obj = UploadHandler()
# obj.setDbPathOrUrl("databases/relational.db")
# print(obj.pushDataToDb("data/process.json"))
##############
class MetadataUploadHandler(UploadHandler): # (i.UploadHandler):  fix author (can moren than 1) and date (in case it's 0)
    
    def pushDataToDb(self, path : str) -> bool:
        blzgrph = SPARQLUpdateStore()
        endpoint = self.getDbPathOrURL() # if you try this, remember to update the endpoint depending on the one set when running blzgraph
        print(endpoint)
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
            return result["boolean"]
            


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
                
                #added some general if clause to include that the triple isn't considered if the data is missing, unless it's the date that can be absent
                if len(obj) == 0 and pred != "datePublished":
                    pass
                elif pred == "datePublished" and len(obj) == 0:
                    if not(check_yoself_befo_yo_shrek_yoself(subj, SDO+pred, obj)):
                                graph_to_upload.add((rdf.URIRef(subj),rdf.URIRef(SDO+pred),rdf.Literal(obj)))
                else:
                    if pred=="author":
                        if len(obj)==0:
                                pass
                        else:
                            list_of_auth = []
                            if ";" in obj:
                                list_of_auth.extend(obj.split("; "))
                            else:
                                list_of_auth.append(obj)
                            
                            for single_auth in list_of_auth:
                                obj_list = single_auth.split(" (")
                                name=obj_list[0] #assigning the actual name to this variable
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
        
        try:                                                                     #added try condition that results in a boolean output
            blzgrph.open((endpoint,endpoint))
            for sent in graph_to_upload.triples((None,None,None)):
                blzgrph.add(sent)
            blzgrph.close()
            return True
        except:
            return False


############ QUERY MANAGEMENT #################
    
class QueryHandler(Handler):

    def __init__(self):
        super().__init__()
    
    def getById(self, id: str) -> pd.DataFrame:
        try:
            self.endpoint = super().getDbPathOrURL()
            self.request = sw.SPARQLWrapper(self.endpoint)
            self.request.setReturnFormat(sw.JSON)
            self.request.setQuery(f"""
            SELECT ?name ?type ?date ?namePlace ?nameOwner ?nameAuthor
            WHERE {{ ?uri <https://schema.org/identifier>  "{id}" . 
            {{ ?uri <https://schema.org/name> ?name ; 
                    rdf:type ?typeUri ;
                    <https://schema.org/identifier> ?id .
                ?typeUri rdfs:label ?type .
                ?uri <https://schema.org/datePublished> ?date ;
                     <https://schema.org/spatial> ?uriPlace ;
                     <https://schema.org/maintainer> ?uriOwner .
                ?uriPlace rdfs:label ?namePlace .
                ?uriOwner rdfs:label ?nameOwner .
                OPTIONAL {{ ?uri <https://schema.org/author> ?uriAuthor .
                ?uriAuthor <https://schema.org/givenName> ?nameAuthor . }}
                 }} UNION {{ ?uri <https://schema.org/givenName> ?name }} . }}
            """)
            result = self.request.query().convert()
            result = result["results"]["bindings"]
            for row in result:
                if id.isdigit():
                    if "nameAuthor" in list(row.keys()) and "date" in list(row.keys()):
                        result_df = pd.DataFrame({"Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}) 
                    elif "nameAuthor" not in list(row.keys()) and "date" in list(row.keys()):
                        result_df = pd.DataFrame({"Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([""]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}) 
                    elif "nameAuthor" in list(row.keys()) and "date" not in list(row.keys()):
                        result_df = pd.DataFrame({"Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([""]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})
                    elif "nameAuthor" not in list(row.keys()) and "date" not in list(row.keys()):
                        result_df = pd.DataFrame({"Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([""]), "Date Publishing": pd.Series([""]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})
                else:
                    result_df = pd.DataFrame({"Person": pd.Series([row["name"]["value"]])})
            return result_df
        except Exception:
            return(Exception)

class ProcessDataQueryHandler(QueryHandler):

    def getAllActivities(self) -> pd.DataFrame:
        try:    
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute("""SELECT * FROM acquisitionData UNION 
                        SELECT * FROM processingData UNION
                        SELECT * FROM modellingData UNION
                        SELECT * FROM optimisingData UNION
                        SELECT * FROM exportingData""")
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()
    
    def getActivitiesByResponsibleInstitution(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute(f"""SELECT * FROM acquisitionData WHERE responsible_institute LIKE '%{input_string}%' UNION
                           SELECT * FROM processingData WHERE responsible_institute LIKE '%{input_string}%' UNION
                           SELECT * FROM modellingData WHERE responsible_institute LIKE '%{input_string}%' UNION
                           SELECT * FROM optimisingData WHERE responsible_institute LIKE '%{input_string}%' UNION
                           SELECT * FROM exportingData WHERE responsible_institute LIKE '%{input_string}%'
                            """)
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()

    def getActivitiesByResponsiblePerson(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute(f"""SELECT * FROM acquisitionData WHERE responsible_person LIKE '%{input_string}%' UNION
                           SELECT * FROM processingData WHERE responsible_person LIKE '%{input_string}%' UNION
                           SELECT * FROM modellingData WHERE responsible_person LIKE '%{input_string}%' UNION
                           SELECT * FROM optimisingData WHERE responsible_person LIKE '%{input_string}%' UNION
                           SELECT * FROM exportingData WHERE responsible_person LIKE '%{input_string}%'
                            """)
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()
        
    def getActivitiesUsingTool(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute(f"""SELECT * FROM acquisitionData WHERE tool LIKE '%{input_string}%' UNION
                           SELECT * FROM processingData WHERE tool LIKE '%{input_string}%' UNION
                           SELECT * FROM modellingData WHERE tool LIKE '%{input_string}%' UNION
                           SELECT * FROM optimisingData WHERE tool LIKE '%{input_string}%' UNION
                           SELECT * FROM exportingData WHERE tool LIKE '%{input_string}%'
                            """)
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()
        
    def getActivitiesStartedAfter(self, input_date: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            input_datetime = datetime.strptime(input_date, '%Y-%m-%d')
            cursor.execute("""SELECT * FROM acquisitionData WHERE start_date >= ?
                            UNION
                            SELECT * FROM processingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM modellingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM optimisingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM exportingData WHERE start_date >= ?""", (input_datetime, input_datetime, input_datetime, input_datetime, input_datetime))
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()

    def getActivitiesEndedBefore(self, input_date: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            input_datetime = datetime.strptime(input_date, '%Y-%m-%d')
            cursor.execute("""SELECT * FROM acquisitionData WHERE end_date <= ?
                            UNION
                            SELECT * FROM processingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM modellingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM optimisingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM exportingData WHERE end_date <= ?""", (input_datetime, input_datetime, input_datetime, input_datetime, input_datetime))
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()
        
    def getAcquisitionsByTechnique(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrURL()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM acquisitionData WHERE technique LIKE '%{input_string}%'")
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("File not found. Try specifying a different path")
            return pd.DataFrame()

class MetadataQueryHandler(QueryHandler):  
    def __init__(self):  
        super().__init__()  
    
    # Step 2. set query, send it and convert the result, create a dynamical dataframe getting every information from the JSON file using one-line for-loops
    def getAllPeople(self) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrURL()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.request.setQuery("""
        SELECT ?name ?id ?uri
        WHERE { ?uri <https://schema.org/givenName> ?name ;
                     <https://schema.org/identifier> ?id . }
        """)
        result = self.request.query().convert()
        result = result["results"]["bindings"]
        result_df = pd.DataFrame({"Name": pd.Series([row["name"]["value"] for row in result]), "Id": pd.Series([row["id"]["value"] for row in result]), 
                          "Uri": pd.Series([row["uri"]["value"]] for row in result)})
        return result_df

    # Step 3. do it again  - in this case the query is more complex
    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrURL()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.query = """
        SELECT ?obj ?type ?id ?uri ?nameAuthor ?date ?namePlace ?nameOwner 
        WHERE { ?uri <https://schema.org/name> ?obj ;
                     rdf:type ?typeUri ;
                     <https://schema.org/identifier> ?id .
                ?typeUri rdfs:label ?type .
                ?uri <https://schema.org/datePublished> ?date ;
                     <https://schema.org/spatial> ?uriPlace ;
                     <https://schema.org/maintainer> ?uriOwner .
                ?uriPlace rdfs:label ?namePlace .
                ?uriOwner rdfs:label ?nameOwner .
                OPTIONAL { ?uri <https://schema.org/author> ?uriAuthor .
                ?uriAuthor <https://schema.org/givenName> ?nameAuthor . }
                 }
        """
        
        self.request.setQuery(self.query)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_rows = []

        #I'm keeping it just in case :) - DON'T DELETE IT!!!
        # self.result_df = pd.DataFrame({"Object": pd.Series([row["obj"]["value"] for row in self.result]), "Type": pd.Series([row["type"]["value"] for row in self.result]),
        #                                "Id": pd.Series([row["id"]["value"] for row in self.result]), "Uri": pd.Series([row["uri"]["value"] for row in self.result]),
        #                                "Author": pd.Series([row["nameAuthor"]["value"] for row in self.result]), "Date Publishing": pd.Series([row["date"]["value"] for row in self.result]),
        #                                "Place": pd.Series([row["namePlace"]["value"] for row in self.result]), "Owner": pd.Series([row["nameOwner"]["value"] for row in self.result])}) 

        for row in self.result:
            if "nameAuthor" in list(row.keys()) and "date" in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([int(row["id"]["value"])]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})) 
            elif "nameAuthor" not in list(row.keys()) and "date" in list(row.keys()):
               self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([int(row["id"]["value"])]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([""]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})) 
            elif "nameAuthor" in list(row.keys()) and "date" not in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([int(row["id"]["value"])]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([""]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
            elif "nameAuthor" not in list(row.keys()) and "date" not in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([int(row["id"]["value"])]), "Uri": pd.Series([row["uri"]["value"] for row in self.result]),
                                       "Author": pd.Series([""]), "Date Publishing": pd.Series([""]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
        
        self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)
        dfs_to_concat = []
        # print (pd.pivot_table(self.result_df, index=["Id"], aggfunc="size"))

        for id, num in pd.pivot_table(self.result_df, index=["Id"], aggfunc="size").items(): # for each object that appears more than once
            if int(num) > 1:
                item_to_search = [id]

                mask = self.result_df["Id"].isin(item_to_search) 
                df_of_mult_authors = self.result_df[mask]                 #bring me a selection of the df in which it appears
                # print(df_of_mult_authors)
                all_auth_of_curr_obj = df_of_mult_authors["Author"].tolist()  #list of the authors
                rows_to_drop = df_of_mult_authors.index.values.tolist()      #list of the indexes
                # print (all_auth_of_curr_obj, rows_to_drop)
                auth = "; ".join(all_auth_of_curr_obj)                      #create one string for the author
                row_to_insert = self.result_df.loc[[rows_to_drop[0]]].copy()                          #take one of the rows and copy it
                # print("row to insert before:", row_to_insert)
                row_to_insert.at[rows_to_drop[0],"Author"] = auth               #modify the authors putting in the single string with all of them
                # print("row to insert after", row_to_insert)
                dfs_to_concat.append(row_to_insert)                            #we'll concateate that row
                self.result_df = self.result_df.drop(x for x in rows_to_drop)      #drop all the rows with that id that are now useless
                # print(self.result_df)
        
        dfs_to_concat.append(self.result_df)                                 #add the df with the deleted rows in the list that will be concatenated
        result = pd.concat(dfs_to_concat, join="outer", ignore_index=True)   #concatenate all the rows with the df
        # print (result[["Author", "Object"]])
        return result
    
    # Step 4. do it again. But this time, use the f-string to insert dinamically the object to seach
    def getAuthorsOfCulturalHeritageObject(self, objectId : str) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrURL()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.request.setQuery(f"""
        SELECT ?name ?id ?uri
        WHERE {{ ?uri <https://schema.org/givenName> ?name ;
                     <https://schema.org/identifier> ?id .
                 ?objUri <https://schema.org/author> ?uri;
                         <https://schema.org/identifier> '{objectId}' . }}
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_df = pd.DataFrame({"Name": pd.Series([row["name"]["value"] for row in self.result]), "Id": pd.Series([row["id"]["value"] for row in self.result]), 
                          "Uri": pd.Series([row["uri"]["value"]] for row in self.result)})
        return self.result_df
    
    # Step 5. someone stop me (I've done it again)
    def getCulturalHeritageObjectsAuthoredBy(self, personId : str) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrURL()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.request.setQuery(f"""
        SELECT ?obj ?type ?id ?uri
        WHERE {{ ?uri <https://schema.org/name> ?obj ;
                     rdf:type ?typeUri ;
                     <https://schema.org/identifier> ?id ;
                     <https://schema.org/author> ?persUri .
                 ?typeUri rdfs:label ?type .
                 ?persUri <https://schema.org/identifier> '{personId}' . }}
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_df = pd.DataFrame({"Object": pd.Series([row["obj"]["value"] for row in self.result]), "Type": pd.Series([row["type"]["value"] for row in self.result]),
                                       "Id": pd.Series([row["id"]["value"] for row in self.result]), "Uri": pd.Series([row["uri"]["value"] for row in self.result])}) 
        return self.result_df

### Test
#obj = MetadataQueryHandler()
#print(obj.dbPathOrURL)
#obj.setDbPathOrUrl("http://192.168.1.15:9999/blazegraph/sparql")
#print(obj.getAllPeople())
#print(obj.getCulturalHeritageObjectsAuthoredBy("VIAF:100190422"))

############## MASHUP #################

class BasicMashup:

    def __init__(self) -> None:
        self.metadataQuery: list[MetadataQueryHandler] = []
        self.processdataQuery: list[ProcessDataQueryHandler] = []
    
    @staticmethod
    def row_to_obj(s: pd.Series, bm) -> None:
        cult_obj = bm.getEntityById(s["object_id"])
        curr_type: str = s["type"]
        match curr_type: 
            case "acquisition":
                obj = Acquisition(
                                institute=s["responsible_institute"], 
                                technique=s["technique"], 
                                person=s["responsible_person"], 
                                tool=s["tool"], 
                                start=s["start_date"], 
                                end=s["end_date"], 
                                refersTo=cult_obj)
            case "processing":
                obj = Processing(
                                institute=s["responsible_institute"], 
                                person=s["responsible_person"], 
                                tool=s["tool"], 
                                start=s["start_date"], 
                                end=s["end_date"], 
                                refersTo=cult_obj)                                        
            case "modelling":
                obj = Modelling(
                                institute=s["responsible_institute"], 
                                person=s["responsible_person"], 
                                tool=s["tool"], 
                                start=s["start_date"], 
                                end=s["end_date"], 
                                refersTo=cult_obj)
            case "optimising":
                obj = Optimising(
                                institute=s["responsible_institute"], 
                                person=s["responsible_person"], 
                                tool=s["tool"], 
                                start=s["start_date"], 
                                end=s["end_date"], 
                                refersTo=cult_obj)                                        
            case "exporting":
                obj = Exporting(
                                institute=s["responsible_institute"], 
                                person=s["responsible_person"], 
                                tool=s["tool"], 
                                start=s["start_date"], 
                                end=s["end_date"], 
                                refersTo=cult_obj)
        return obj


    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery.clear()
        print("MetaData handlers succesfully reset")
        return True
    
    def cleanProcessHandlers(self) -> bool:
        self.processdataQuery.clear()
        print("ProcessData handlers succesfully reset")
        return True
    
    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool: # maybe checking if the same handler is already in the list?
        try:
            self.metadataQuery.append(handler)
            print("Handler succesfully added to the MetaData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:  # maybe checking if the same handler is already in the list?
        try:
            self.processdataQuery.append(handler)
            print("Handler succesfully added to the ProcessData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False

    def getEntityById(self, id: str) -> IdentifiableEntity | None:
        pass
    def getAllPeople(self, ) -> list[IdentifiableEntity]:
        pass
    def getAllCulturalHeritageObjects(self, ) -> list[CulturalHeritageObject]:
        pass
    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> list[Person]:
        pass
    def getCulturalHeritageObjectsAuthoredBy(self, personalId: str) -> list[CulturalHeritageObject]:
        pass

    @print_attributes
    def getAllActivities(self) -> list[Activity]:
        df_list = [handler.getAllActivities() for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list()

    @print_attributes
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list() 
    
    @print_attributes
    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesUsingTool(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        df_list = [handler.getActivitiesStartedAfter(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        df_list = [handler.getActivitiesEndedBefore(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
        return obj_series.to_list()
    
    @print_attributes
    def getAcquisitionByTechnique(self, partialName: str) -> list[Acquisition]:
        result = []
        df_list = [handler.getAcquisitionByTechnique(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, join="inner", ignore_index=True)
        final_df.drop_duplicates(inplace=True, ignore_index=True)
        for _, row in final_df.iterrows():
            cult_obj = self.getEntityById(row["object_id"])
            obj = Acquisition(
                            institute=row["responsible_institute"], 
                            technique=row["technique"], 
                            person=row["responsible_person"], 
                            tool=row["tool"], 
                            start=row["start_date"], 
                            end=row["end_date"], 
                            refersTo=cult_obj)
            result.append(obj)
        
        return result
    
### TEST ###
# obj = BasicMashup()
# pqh = ProcessDataQueryHandler()
# pqh.setDbPathOrUrl("databases/relational.db")
# obj.addProcessHandler(pqh)
# obj.addProcessHandler(pqh)
# obj.addProcessHandler(pqh)
# obj.getAllActivities()
# print(obj.getActivitiesByResponsibleInstitution("Heritage"))

class AdvancedMashup(BasicMashup): 

    def __init__(self) -> None:
        super().__init__()

    @print_attributes
    def getActivitiesOnObjectsAuthoredBy(self, personId: str): 
        try:
            if len(self.processdataQuery) == 0:
                print("No ProcessdataQueryHandler was specified for the mashup process. Please add at least one")
                return []
            if len(self.metadataQuery) == 0:
                print("No MetadataQueryHandler was specified for the mashup process. Please add at least one")
                return []
            mdf_list = [m_handler.getCulturalHeritageObjectsAuthoredBy(personId) for m_handler in self.metadataQuery]
            pdf_list = [p_handler.getAllActivities() for p_handler in self.processdataQuery]
            m_conc_df = pd.concat(mdf_list, join="outer", ignore_index=True) 
            # print(f"MQlist concatenated:\n{m_conc_df}")
            p_conc_df = pd.concat(pdf_list, join="outer", ignore_index=True)
            # print(f"PQlist concatenated:\n{p_conc_df}")
            final_df = p_conc_df.merge(m_conc_df, how="right", left_on="object_id", right_on="Id")
            final_df.drop_duplicates(inplace=True, ignore_index=True)
            # print(f"Merged df:\n{final_df.head(20)}")
            obj_series = final_df.apply(lambda x: self.row_to_obj(x, self), axis=1)
            return obj_series.to_list()
        except Exception as e:
            print(f"{e}")
            return []

    def getObjectsHandledByResponsiblePerson(self, partialName: str):
        pass
    def getObjectsHandledByResponsibleInstitution(self, partialName: str):
        try:
            if len(self.processdataQuery) == 0:                                                  # checking if there are any PDQHs in the attribute 
                print("No MetadataQueryHandler set for the mashup process. Please add at least one")
                return []
            if len(self.metadataQuery) == 0:                                                      # same but for the MDQHs
                print("No MetadataQueryHandler set for the mashup process. Please add at least one")
                return []
            else:                                        # get the DFs using the corresponding methods using a quick in-line for loop
                pdf_list = [pd_handler.getActivitiesByResponsibleInstitution(partialName) for pd_handler in self.processdataQuery]
                mdf_list = [md_handler.getAllCulturalHeritageObjects() for md_handler in self.metadataQuery]

                p_concat_df = pd.concat(pdf_list, join="outer", ignore_index=True)
                p_concat_df["object_id"]   = p_concat_df["object_id"].astype("int64")            
                m_concat_df = pd.concat(mdf_list, join="outer", ignore_index=True)
                merged_df = pd.merge(p_concat_df, m_concat_df, left_on="object_id", right_on="Id")   # after concatenating the list of DFs, they are merged on the ID of the object
                
                list_to_return = list()  # setting the list that will be returned
                list_id = list()  # setting a list for the comparison of IDs, as to avoid adding multiple objects for the same CHO
                
                for idx, row in merged_df.iterrows():         # for any row in the df an object is created of the instance correspective to their type
                    match row["Type"]:
                        case "Nautical chart":
                            obj_to_append = NauticalChart(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Manuscript plate":
                            obj_to_append = ManuscriptPlate(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Manuscript volume":
                            obj_to_append = ManuscriptVolume(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Printed volume":
                            obj_to_append = PrintedVolume(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Printed material":
                            obj_to_append = PrintedMaterial(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Herbarium":
                            obj_to_append = Herbarium(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Specimen":
                            obj_to_append = Specimen(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Painting":
                            obj_to_append = Painting(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Model":
                            obj_to_append = Model(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                        case "Map":
                            obj_to_append = Map(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"].split("; "))
                    
                    if len(list_to_return) == 0:                   # quick check to see if 1) the result list is empty (add the python object regardless)
                        list_to_return.append(obj_to_append)
                        list_id.append(obj_to_append.getId())
                    else:                                          # and otherwise 2) if there are python objects with the same ID already parsed (in this case, ignore the object)
                        if obj_to_append.getId() not in list_id:
                            list_to_return.append(obj_to_append)
                            list_id.append(obj_to_append.getId())
                        else:
                            pass

                return list_to_return
                
        except Exception as e:
          return f"{e}"
        
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start: str, end: str):
        pass
