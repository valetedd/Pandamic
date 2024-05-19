import pandas as pd
from sqlite3 import connect, OperationalError
import json
from util import *
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw
from datetime import datetime
from pprint import pprint

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

    def getDbPathOrUrl(self):
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
        
        
class UploadHandler(Handler):

    def pushDataToDb(self, path: str) -> bool:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
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
            db = self.getDbPathOrUrl()        
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
            print("Connection to db failed. Try resetting the db path or check the well-formedness of the JSON file")
            return False
        except TypeError:
            print("Please specify a valid path for the JSON file you want to upload")
            return False
        except Exception as e:
            print(f"{e}")
            return False

class MetadataUploadHandler(UploadHandler): # (i.UploadHandler):  fix author (can moren than 1) and date (in case it's 0)
    
    def pushDataToDb(self, path : str) -> bool:
        blzgrph = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl() # if you try this, remember to update the endpoint depending on the one set when running blzgraph
        def check_if_triples_exists(subj, pred, obj):
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
        graph_to_upload.bind("pdm", PDM)                                           # to remove!
        
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
            title = row["name"]
            subj = PDM + row["identifier"] # generating a specific URI for each item
            
            for pred, obj in row.items():
                
                #added some general if clause to include that the triple isn't considered if the data is missing, unless it's the date that can be absent
                if len(obj) == 0 and pred != "datePublished" and len(obj) == 0 and pred != "author":
                    print(f"the item {title} doesn't conform to the model due to missing data. This object won't be returned by Mashup methods")
                # elif pred == "datePublished" and len(obj) == 0:
                #     if not(check_if_triples_exists(subj, SDO+pred, obj)):
                #                 graph_to_upload.add((rdf.URIRef(subj),rdf.URIRef(SDO+pred),rdf.Literal(obj)))
                elif len(obj) == 0 and pred == "datePublished" or len(obj) == 0 and pred == "author":
                    pass
                else:
                    if pred=="author":
                        list_of_auth = []
                        list_of_auth.extend(obj.split("; "))
                        
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
                            
                            if not(check_if_triples_exists(author_url, SDO.identifier, author_ID)):
                                graph_to_upload.add((rdf.URIRef(author_url),SDO.identifier,rdf.Literal(author_ID))) # one triple for each author: author's URI-its ID

                            if not(check_if_triples_exists(subj, SDO.author, author_url)):
                                graph_to_upload.add((rdf.URIRef(subj),SDO.author,rdf.URIRef(author_url)))          # URI of the book-URI of the author

                            if not(check_if_triples_exists(author_url, SDO.givenName, name)):
                                graph_to_upload.add((rdf.URIRef(author_url),SDO.givenName,rdf.Literal(name)))  # URI of the author - its name
                    else:
                        if pred == "type":
                            if not(check_if_triples_exists(subj, RDF.type, dict_of_obj_uri[obj])):
                                graph_to_upload.add((rdf.URIRef(subj),RDF.type,rdf.URIRef(dict_of_obj_uri[obj]))) # 2 triples, book's URI - type's URI,
                            
                            if not(check_if_triples_exists(dict_of_obj_uri[obj], RDFS.label, obj)):
                                graph_to_upload.add((rdf.URIRef(dict_of_obj_uri[obj]),RDFS.label,rdf.Literal(obj)))  # type's URI - type's name. Since
                            # graphs don't allow redundancy, only one of the second triple will be added for each type.

                        elif pred == "maintainer" or pred=="spatial":
                            if not(check_if_triples_exists(subj, SDO+pred, dict_of_obj_uri[obj])):
                                graph_to_upload.add((rdf.URIRef(subj), rdf.URIRef(SDO+pred), rdf.URIRef(dict_of_obj_uri[obj]))) #Same concept for the other 2, but
                            
                            if not(check_if_triples_exists(dict_of_obj_uri[obj], RDFS.label, obj)):
                                graph_to_upload.add((rdf.URIRef(dict_of_obj_uri[obj]), RDFS.label, rdf.Literal(obj)))  # they use SDO instead of RDF
                        
                        else:
                           if not(check_if_triples_exists(subj, SDO+pred, obj)):
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
            self.endpoint = super().getDbPathOrUrl()
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
            return pd.DataFrame()

class ProcessDataQueryHandler(QueryHandler):

    def getAllActivities(self) -> pd.DataFrame:
        try:    
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
    
    def getActivitiesByResponsibleInstitution(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()

    def getActivitiesByResponsiblePerson(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
        
    def getActivitiesUsingTool(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
        
    def getActivitiesStartedAfter(self, input_date: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()

    def getActivitiesEndedBefore(self, input_date: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
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
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
        
    def getAcquisitionInPeriod(self, startTime, endTime) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
            conn = connect(db)
            cursor = conn.cursor()
            start_datetime = datetime.strptime(startTime, '%Y-%m-%d')
            end_datetime = datetime.strptime(endTime,'%Y-%m-%d')
            cursor.execute("""SELECT * FROM acquisitionData WHERE start_date >=? AND end_date <= ?""", (start_datetime,end_datetime))
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
        
    def getAcquisitionsByTechnique(self, input_string: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM acquisitionData WHERE technique LIKE '%{input_string}%'")
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            return pd.DataFrame(data, columns = columns)
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()

class MetadataQueryHandler(QueryHandler):  
    def __init__(self):   # Step 1. first of all, i set a fixed endpoint and format to return
        super().__init__()  
    
    # Step 2. set query, send it and convert the result, create a dynamical dataframe getting every information from the JSON file using one-line for-loops
    def getAllPeople(self) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrUrl()
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
        self.endpoint = super().getDbPathOrUrl()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.query = """
        SELECT ?obj ?type ?id ?uri ?nameAuthor ?date ?namePlace ?nameOwner 
        WHERE { ?uri <https://schema.org/name> ?obj ;
                     rdf:type ?typeUri ;
                     <https://schema.org/identifier> ?id .
                ?typeUri rdfs:label ?type .
                ?uri <https://schema.org/spatial> ?uriPlace ;
                     <https://schema.org/maintainer> ?uriOwner .
                ?uriPlace rdfs:label ?namePlace .
                ?uriOwner rdfs:label ?nameOwner .
                OPTIONAL { ?uri <https://schema.org/author> ?uriAuthor .
                ?uriAuthor <https://schema.org/givenName> ?nameAuthor . 
                ?uri <https://schema.org/datePublished> ?date . }
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
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})) 
            elif "nameAuthor" not in list(row.keys()) and "date" in list(row.keys()):
               self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([""]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})) 
            elif "nameAuthor" in list(row.keys()) and "date" not in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([""]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
            elif "nameAuthor" not in list(row.keys()) and "date" not in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"] for row in self.result]),
                                       "Author": pd.Series([""]), "Date Publishing": pd.Series([""]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
        try:
            self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)
        except:
            self.result_df = pd.DataFrame(columns=["Object", "Type", "Id", "Uri", "Author", "Date Publishing", "Place", "Owner"])

        return self.result_df
    
    # Step 4. do it again. But this time, use the f-string to insert dinamically the object to seach
    def getAuthorsOfCulturalHeritageObject(self, objectId : str) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrUrl()
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
        self.result_rows = []

        for row in self.result:
            self.result_rows.append(pd.DataFrame({"Name": pd.Series([row["name"]["value"] for row in self.result]), "Id": pd.Series([row["id"]["value"] for row in self.result]), 
                          "Uri": pd.Series([row["uri"]["value"]] for row in self.result)}))
        try:
            self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)
        except:
            self.result_df = pd.DataFrame(columns=["Name", "Id", "Uri"])

        return self.result_df

    # Step 5. someone stop me (I've done it again)
    def getCulturalHeritageObjectsAuthoredBy(self, personId : str) -> pd.DataFrame:
        self.endpoint = super().getDbPathOrUrl()
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
        self.request.setQuery(f"""
        SELECT ?obj ?type ?id ?uri ?date ?nameAuthor ?namePlace ?nameOwner
        WHERE {{ ?uri <https://schema.org/name> ?obj ;
                      rdf:type ?typeUri ;
                      <https://schema.org/identifier> ?id ;
                      <https://schema.org/author> ?persUri .
                 ?typeUri rdfs:label ?type .
                 ?uri <https://schema.org/spatial> ?uriPlace ;
                      <https://schema.org/maintainer> ?uriOwner .
                 ?uriPlace rdfs:label ?namePlace .
                 ?uriOwner rdfs:label ?nameOwner .
                 ?persUri <https://schema.org/identifier> '{personId}' . 
                 ?persUri <https://schema.org/givenName> ?nameAuthor .
                 OPTIONAL {{?uri <https://schema.org/datePublished> ?date . }}
                 }}
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]

        self.result_rows = []

        for row in self.result:
            if "date" in list(row.keys()):
                self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                       "Author": pd.Series([row["nameAuthor"]["value"]]), "Author Id": pd.Series([personId]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})) 
            elif "date" not in list(row.keys()):
               self.result_rows.append(pd.DataFrame({"Object": pd.Series([row["obj"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]), "Date Publishing": pd.Series([""]),
                                        "Author": pd.Series([row["nameAuthor"]["value"]]), "Author Id": pd.Series([personId]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
        try:
            self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)
        except:
            self.result_df = pd.DataFrame(columns=["Object", "Type", "Id", "Uri", "Date Publishing", "Place", "Owner"])
            
        return self.result_df

############## MASHUP #################

class BasicMashup:

    def __init__(self) -> None:
        self.metadataQuery: list[MetadataQueryHandler] = []
        self.processdataQuery: list[ProcessDataQueryHandler] = []
    
    @classmethod
    def row_to_obj(cls, s: pd.Series, refersTo_dict: dict) -> Activity:
        cult_obj = refersTo_dict.get(s["object_id"])
        curr_type = s["type"]
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
    
    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool: 
        try:
            if not isinstance(handler, MetadataQueryHandler):
                raise ValueError("Input has to be a MetadataQueryHandler object")
            self.metadataQuery.append(handler)
            print("Handler succesfully added to the MetaData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:  
        try:
            if not isinstance(handler, ProcessDataQueryHandler):
                raise ValueError("Input has to be a ProcessDataQueryHandler object")
            self.processdataQuery.append(handler)
            print("Handler succesfully added to the ProcessData handlers-list")
            return True
        except TypeError:
            print("Please specify a handler to be added")
            return False
        
    @print_attributes
    def getEntityById(self, id: str) -> Person | CulturalHeritageObject | None:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getById(id)
            
            if not df_got.empty:
                df_list.append(df_got)

        if not df_list: 
            return None       
        df = pd.concat(df_list).dropna(how='all').reset_index(drop=True)
        
        if not df.empty:
            try:
                df = df.squeeze()
                object_type = df['Type']
                match object_type:
                    case "Nautical chart":
                        return NauticalChart(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Printed volume":
                        return PrintedVolume(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Herbarium":
                        return Herbarium(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Printed material":
                        return PrintedMaterial(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Specimen":
                        return Specimen(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Painting":
                        return Painting(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Map":
                        return Map(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Manuscript volume":
                        return ManuscriptVolume(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Manuscript plate":
                        return ManuscriptPlate(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    case "Model":
                        return Model(id,df['Object'],df['Date Publishing'],df['Owner'],df['Place'],df['Author'])
                    
            except:
                name = df
                return Person(id,name)
            
        else:
            return None 
        
    @print_attributes    
    def getAllPeople(self) -> list[Person]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getAllPeople()
            df_list.append(df_got)

        person_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Name' in row and 'Id' in row:
                    person_list.append(Person(row['Id'], row['Name']))

        return person_list

    @print_attributes
    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getAllCulturalHeritageObjects()
            df_list.append(df_got)

        culturalHeritageObject_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Type' in row:
                    df = df.squeeze()
                    obj = None
                    object_type = row['Type']
                    if object_type == "Nautical chart":
                        obj = NauticalChart(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Printed volume":
                        obj = PrintedVolume(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Herbarium":
                        obj = Herbarium(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Printed material":
                        obj = PrintedMaterial(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Specimen":
                        obj = Specimen(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Painting":
                        obj = Painting(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Map":
                        obj = Map(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Manuscript volume":
                        obj = ManuscriptVolume(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Manuscript plate":
                        obj = ManuscriptPlate(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Model":
                        obj = Model(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    
                    if obj:
                        culturalHeritageObject_list.append(obj)
 
        return culturalHeritageObject_list
    
    @print_attributes
    def getAuthorsOfCulturalHeritageObject(self, id: str) -> list[Person]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getAuthorsOfCulturalHeritageObject(id)
            df_list.append(df_got)
        
        

        person_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Name' in row and 'Id' in row:
                    person_list.append(Person(row['Id'],row['Name']))

        return person_list
    
    @print_attributes
    def getCulturalHeritageObjectsAuthoredBy(self, id: str) -> list[CulturalHeritageObject]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getCulturalHeritageObjectsAuthoredBy(id)
            df_list.append(df_got)        

        culturalHeritageObject_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Type' in row:
                    df = df.squeeze()
                    obj = None
                    object_type = row['Type']
                    if object_type == "Nautical chart":
                        obj = NauticalChart(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Printed volume":
                        obj = PrintedVolume(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Herbarium":
                        obj = Herbarium(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Printed material":
                        obj = PrintedMaterial(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Specimen":
                        obj = Specimen(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Painting":
                        obj = Painting(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Map":
                        obj = Map(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Manuscript volume":
                        obj = ManuscriptVolume(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Manuscript plate":
                        obj = ManuscriptPlate(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    elif object_type == "Model":
                        obj = Model(df['Id'], df['Object'], df['Date Publishing'], df['Owner'], df['Place'], df['Author'])
                    
                    if obj:
                        culturalHeritageObject_list.append(obj)
 
        return culturalHeritageObject_list

    @print_attributes
    def getAllActivities(self) -> list[Activity]:
        df_list = [handler.getAllActivities() for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list()

    @print_attributes
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list() 
    
    @print_attributes
    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]:
        df_list = [handler.getActivitiesUsingTool(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        df_list = [handler.getActivitiesStartedAfter(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    @print_attributes
    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        df_list = [handler.getActivitiesEndedBefore(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {ext_id:self.getEntityById(ext_id) for ext_id in unique_ids}
        obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    @print_attributes
    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]:
        result = []
        df_list = [handler.getAcquisitionsByTechnique(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True)
        if len(self.processdataQuery) > 1:
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

class AdvancedMashup(BasicMashup): 

    def __init__(self) -> None:
        super().__init__()

    @print_attributes
    def getActivitiesOnObjectsAuthoredBy(self, personId: str): 
        try:
            if (len_pq := len(self.processdataQuery)) == 0:
                raise AttributeError("No ProcessdataQueryHandler was specified for the AdvancedMashup process. Please add at least one")
            if (len_mq := len(self.metadataQuery)) == 0:
                raise AttributeError("No MetadataQueryHandler was specified for the AdvancedMashup process. Please add at least one")
            
            mdf_list = [m_handler.getCulturalHeritageObjectsAuthoredBy(personId) for m_handler in self.metadataQuery]
            pdf_list = [p_handler.getAllActivities() for p_handler in self.processdataQuery]
            m_conc_df = pd.concat(mdf_list, ignore_index=True) 
            p_conc_df = pd.concat(pdf_list, ignore_index=True)
            if p_conc_df.empty or m_conc_df.empty:
                return []
            final_df = p_conc_df.merge(m_conc_df, how="right", left_on="object_id", right_on="Id")
            if len_mq > 1 or len_pq > 1:
                final_df.drop_duplicates(inplace=True, ignore_index=True)
            unique_ids = final_df["object_id"].unique()
            cult_obj_dict = {obj_id : entity
                                for obj_id in unique_ids 
                                if (entity := self.getEntityById(obj_id) is not None)}
            obj_series = final_df.apply(lambda row: BasicMashup.row_to_obj(row, cult_obj_dict), axis=1, result_type="reduce")
            return obj_series.to_list()
        except AttributeError:
            return []
        except TypeError:
            print("Please specify an input Id")
            return []
        except Exception as e:
            print(f"{e}")
            return []
        
    @print_attributes
    def getObjectsHandledByResponsiblePerson(self, person: str) -> list[CulturalHeritageObject]:

        id_set = set()
        for handler in self.processdataQuery:
            df_got = handler.getActivitiesByResponsiblePerson(person)
            id = df_got["object_id"].tolist()
            id_set.update(id)
        

        df_list  =[]
        for id in id_set:
            df_got = self.getEntityById(id)
            df_list.append(df_got)
        return df_list
    
    @print_attributes
    def getObjectsHandledByResponsibleInstitution(self, partialName: str):
        try:
            if len(self.processdataQuery) == 0:                                                  # checking if there are any PDQHs in the attribute 
                print("No MetadataQueryHandler set for the AdvancedMashup process. Please add at least one")
                return []
            if len(self.metadataQuery) == 0:                                                      # same but for the MDQHs
                print("No MetadataQueryHandler set for the AdvancedMashup process. Please add at least one")
                return []
            else:                                        # get the DFs using the corresponding methods using a quick in-line for loop
                pdf_list = [pd_handler.getActivitiesByResponsibleInstitution(partialName) for pd_handler in self.processdataQuery]
                mdf_list = [md_handler.getAllCulturalHeritageObjects() for md_handler in self.metadataQuery]

                p_concat_df = pd.concat(pdf_list, join="outer", ignore_index=True)              
                m_concat_df = pd.concat(mdf_list, join="outer", ignore_index=True)
                merged_df = pd.merge(p_concat_df, m_concat_df, left_on="object_id", right_on="Id")   # after concatenating the list of DFs, they are merged on the ID of the object
                
                list_to_return = list()  # setting the list that will be returned
                list_id = list()  # setting a list for the comparison of IDs, as to avoid adding multiple objects for the same CHO
                
                for idx, row in merged_df.iterrows():         # for any row in the df an object is created of the instance correspective to their type
                    match row["Type"]:
                        case "Nautical chart":
                            obj_to_append = NauticalChart(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Manuscript plate":
                            obj_to_append = ManuscriptPlate(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Manuscript volume":
                            obj_to_append = ManuscriptVolume(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Printed volume":
                            obj_to_append = PrintedVolume(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Printed material":
                            obj_to_append = PrintedMaterial(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Herbarium":
                            obj_to_append = Herbarium(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Specimen":
                            obj_to_append = Specimen(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Painting":
                            obj_to_append = Painting(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Model":
                            obj_to_append = Model(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                        case "Map":
                            obj_to_append = Map(id=str(row["Id"]), title=row["Object"], date=str(row["Date Publishing"]), owner=row["Owner"], place=row["Place"], hasAuthor=row["Author"])
                    
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
        
    @print_attributes  
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, startTime:str, endTime: str) -> list[Person]:

        id_set = set()
        for handler in self.processdataQuery:
            df_got = handler.getAcquisitionInPeriod(startTime, endTime)
            id = df_got["object_id"].tolist()
            id_set.update(id)
        
        author_list = []
        for id in id_set:
            author =  self.getAuthorsOfCulturalHeritageObject(id)
            author_list.extend(author)
        return author_list