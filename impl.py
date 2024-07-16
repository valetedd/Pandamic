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
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
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
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class ManuscriptPlate(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class ManuscriptVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class PrintedVolume(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class PrintedMaterial(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class Herbarium(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class Specimen(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class Painting(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class Model(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)
        
class Map(CulturalHeritageObject):
    def __init__(self, id:str, title: str, owner: str, place: str, date: str = "", hasAuthor: list = []):
        super().__init__(id, title, owner, place, date, hasAuthor)

class Activity():
    def __init__(self, institute:  str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
        self.institute = institute
        self.person = person
        self.tool = tool
        self.start = start
        self.end = end
        self.ch_obj = refersTo
    
    def getResponsibleInstitute(self) -> str:
        return self.institute
    
    def getResponsiblePerson(self) -> str | None:
        return self.person if self.person else None
    
    def getTools(self) -> set[str]:
        return set(self.tool.split(", ")) if self.tool else set("")
    
    def getStartDate(self) -> str | None:
        return self.start if self.start else None
    
    def getEndDate(self) -> str | None:
        return self.end if self.end else None
    
    def refersTo(self) -> CulturalHeritageObject:
        return self.ch_obj
    
class Acquisition(Activity):
    def __init__(self, institute: str, technique: str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.technique = technique
        
    def getTechnique(self):
        return self.technique
    
class Processing(Activity):
    def __init__(self, institute: str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Modelling(Activity):
    def __init__(self, institute: str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Optimising(Activity):
    def __init__(self, institute: str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)

class Exporting(Activity):
    def __init__(self, institute: str, person: str, tool: str, start: str, end: str, refersTo: CulturalHeritageObject):
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
                raise ValueError("Unsupported format. Only .csv or .json files can be specified")
        except ValueError as e:
            print(e)
            return False
        except FileNotFoundError:
            print("File not found. Try specifying a different path")
            return False
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return False
        except Exception as e:
            print(e)
            return False
            
class ProcessDataUploadHandler(UploadHandler):

    def pushDataToDb(self, json_path: str) -> bool:

        try:
            # Loading data into a DataFrame
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)             
            act_df = njson_to_df(data,
                                 attributes=("responsible institute", "responsible person", "technique", "tool", "start date", "end date"),
                                 id_key="object id",  
                                 types=("acquisition", "processing", "modelling", "optimising", "exporting")) # Converting json to Dataframe according to the data model
            act_df = act_df.map(regularize_data) # Regularizing datatypes
            
            # Adding column of stable hashes as internal IDs
            int_ids = hash_ids_for_df(act_df, prefix="act-")
            act_df.insert(0, "internal_id",int_ids) 

            # Uploading data to the selected db       
            db = self.getDbPathOrUrl()        
            types = act_df["type"].unique()
            with connect(db) as con:
                cur = con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table';") # getting existing table names in db
                names_table = cur.fetchall()
                names_list = [row[0] for row in names_table if names_table] # storing them in a list
                for t in types:
                    sdf = act_df[act_df["type"] == t] # dividing data in sub-dataframes by type  
                    df_name = f"{t}Data" # table name to use in the db
                    if df_name in names_list:
                        sdf.to_sql(df_name, con, if_exists="append", index=False) # appending values to existing table
                        cur.execute(f"""CREATE TABLE new AS 
                                        SELECT * FROM {df_name} 
                                        GROUP BY internal_id
                                        ORDER BY CAST(object_id AS INT)""") # creating a new table from existing one, removing duplicates
                        cur.execute(f"""DROP TABLE {df_name};""") # deleting old table
                        cur.execute(f"""ALTER TABLE new RENAME TO {df_name}""") # renaming new table to df_name
                    else:
                        sdf.to_sql(df_name, con, index=False)
            return True
        
        except ValueError as e:
            print(e)
            return False
        except FileNotFoundError:
            print("File not found. Try specifying a different path")
            return False
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return False
        except KeyError as e:
            print(f"{e}: json data is not well-formed")
            return False
        except Exception as e:
            print(e)
            return False
        

class MetadataUploadHandler(UploadHandler): 
    
    def pushDataToDb(self, path : str) -> bool:
        blzgrph = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl() # get the endpoint through heritance

        def check_if_triples_exists(subj, pred, obj):     #with this function I'm checking if the triple i'm parsing is already present in my DB.
            if "http" not in obj:
                obj = '"'+obj+'"'
            else:
                obj = "<"+obj+">"
            request = sw.SPARQLWrapper(endpoint)               # I'm using SPARQLWrapper to query. 
            base_query = f"ASK {{ <{subj}> <{pred}> {obj} . }} "
            request.setReturnFormat(sw.JSON)
            request.setQuery(base_query)
            result = request.query().convert()
            return result["boolean"]
        

        md_Series = pd.read_csv(path, keep_default_na=False, dtype={
            "Id":"string","Type":"string","Title":"string","Date":"string","Author":"string","Owner":"string","Place":"string"
        })
        md_Series= md_Series.rename(columns={"Id":"identifier","Type":"type","Title":"name","Date":"datePublished","Author":"author","Owner":"maintainer","Place":"spatial"})
        
        PDM = rdf.Namespace("http://github.com/valetedd/Pandamic/")  # our base URI
        graph_to_upload=rdf.Graph()                                # creating the graph that i will upload
        
        dict_of_obj_uri=dict()      
        our_obj = list(md_Series["type"].unique())
        our_obj.extend(list(md_Series["maintainer"].unique())) 
        our_obj.extend(list(md_Series["spatial"].unique())) #creating a list with all the unique values from the DF
        for item in our_obj:
            uri_friendly_name=item.replace(" ", "_")
            dict_of_obj_uri[item]=PDM+uri_friendly_name    # adding to the dictionary each entity with its own generated URI.
        
        for idx, row in md_Series.iterrows():
            title = row["name"]
            subj = PDM + row["identifier"] # generating a specific URI for each CHO
            
            for pred, obj in row.items():
                
                # general if clause to include that the triple isn't considered if the data is missing, unless it's 
                # date or author which can be absent following the data model
                if (len(obj) == 0 and pred != "datePublished") and (len(obj) == 0 and pred != "author"):
                    print(f"the item {title} doesn't conform to the model due to missing data. This object won't be returned by Mashup methods")
                
                elif (len(obj) == 0 and pred == "datePublished"):
                    pass
                elif (len(obj) == 0 and pred == "author"):
                    pass
                else:
                    if pred=="author":
                        list_of_auth = []
                        list_of_auth.extend(obj.split("; ")) # create list and populate it with the authors

                        for single_auth in list_of_auth:
                            obj_list = single_auth.split(" (")
                            name=obj_list[0]        # assigning the actual name of the author to this variable
                            author_ID=obj_list[1]
                            author_ID=author_ID[0:-1]
                            if "VIAF" in author_ID:                        # depending on the type of ID we get a differet URI
                                author_uri = "http://viaf.org/" + author_ID.replace(":","/").lower()
                            elif "ULAN" in author_ID:
                                author_uri = "http://vocab.getty.edu/page/" + author_ID.replace(":","/").lower()
                            else:
                                author_uri = PDM + author_ID.replace(":","/").lower()
                            
                            if not(check_if_triples_exists(author_uri, SDO.identifier, author_ID)):
                                graph_to_upload.add((rdf.URIRef(author_uri),SDO.identifier,rdf.Literal(author_ID))) # one triple for each author: author's URI-its ID

                            if not(check_if_triples_exists(subj, SDO.author, author_uri)):
                                graph_to_upload.add((rdf.URIRef(subj),SDO.author,rdf.URIRef(author_uri)))          # URI of the book-URI of the author

                            if not(check_if_triples_exists(author_uri, SDO.givenName, name)):
                                graph_to_upload.add((rdf.URIRef(author_uri),SDO.givenName,rdf.Literal(name)))  # URI of the author - its name
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
            print("Data successfully added to graph database")
            return True
        except:
            return False


############ QUERY MANAGEMENT #################
    
class QueryHandler(Handler):

    def __init__(self):
        super().__init__()
    
    def getById(self, id: str) -> pd.DataFrame:
        try:
            self.endpoint = self.getDbPathOrUrl()
            self.request = sw.SPARQLWrapper(self.endpoint)  # creating a SPARQLWrapper object around the endpoint  
            self.request.setReturnFormat(sw.JSON)

            # dynamic query using f-string
            self.request.setQuery(f"""
            SELECT ?name ?type ?date ?namePlace ?nameOwner ?nameAuthor
            WHERE {{ ?uri <https://schema.org/identifier>  "{id}" . 
              ?uri <https://schema.org/name> ?name ; 
                    rdf:type ?typeUri ;
                    <https://schema.org/identifier> ?id .
                ?typeUri rdfs:label ?type .
                ?uri <https://schema.org/spatial> ?uriPlace ;
                     <https://schema.org/maintainer> ?uriOwner .
                ?uriPlace rdfs:label ?namePlace .
                ?uriOwner rdfs:label ?nameOwner .
                OPTIONAL {{ ?uri <https://schema.org/datePublished> ?date .}}
                OPTIONAL {{?uri <https://schema.org/author> ?uriAuthor .
                    ?uriAuthor <https://schema.org/givenName> ?nameAuthor .}}}}
            """)
            result = self.request.query().convert()
            result = result["results"]["bindings"]
            
            for row in result:                  # if the id is composed by only digits it's a CHO, therefore objects are initialized 
                if id.isdigit():                # depending on the presence/absence of information in the result JSON file
                    if "nameAuthor" in list(row.keys()) and "date" in list(row.keys()):
                        result_df = pd.DataFrame({"Id": pd.Series([id]), "Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}) 
                    elif "nameAuthor" not in list(row.keys()) and "date" in list(row.keys()):
                        result_df = pd.DataFrame({"Id": pd.Series([id]), "Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([""]), "Date Publishing": pd.Series([row["date"]["value"]]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}) 
                    elif "nameAuthor" in list(row.keys()) and "date" not in list(row.keys()):
                        result_df = pd.DataFrame({"Id": pd.Series([id]), "Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([row["nameAuthor"]["value"]]), "Date Publishing": pd.Series([""]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})
                    elif "nameAuthor" not in list(row.keys()) and "date" not in list(row.keys()):
                        result_df = pd.DataFrame({"Id": pd.Series([id]), "Object": pd.Series([row["name"]["value"]]), "Type": pd.Series([row["type"]["value"]]),
                                        "Author": pd.Series([""]), "Date Publishing": pd.Series([""]),
                                        "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])})
                else:                            # otherwise, it's a person. return just their name.
                    result_df = pd.DataFrame({"Id": pd.Series([id]), "Person": pd.Series([row["name"]["value"]])})
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
            cursor.execute("""SELECT * FROM acquisitionData WHERE start_date >= ?
                            UNION
                            SELECT * FROM processingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM modellingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM optimisingData WHERE start_date >= ?
                            UNION
                            SELECT * FROM exportingData WHERE start_date >= ?""", (input_date, input_date, input_date, input_date, input_date))
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            df = pd.DataFrame(data, columns = columns)
            return df 
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()

    def getActivitiesEndedBefore(self, input_date: str) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute("""SELECT * FROM acquisitionData WHERE end_date <= ?
                            UNION
                            SELECT * FROM processingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM modellingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM optimisingData WHERE end_date <= ?
                            UNION
                            SELECT * FROM exportingData WHERE end_date <= ?""", (input_date, input_date, input_date, input_date, input_date))
            data = cursor.fetchall()
            columns = ['internal_id', 'type', 'responsible_institute', 'responsible_person', 'tool','start_date', 'end_date', 'technique','object_id']
            df = pd.DataFrame(data, columns = columns)
            clean_df = df.loc[df["end_date"] != ""]
            return clean_df 
        except OperationalError:
            print("Connection to db failed. Try resetting the db path or check for inconsistencies in your data")
            return pd.DataFrame()
        
    def getAcquisitionInPeriod(self, startTime, endTime) -> pd.DataFrame:
        try:
            db = self.getDbPathOrUrl()
            conn = connect(db)
            cursor = conn.cursor()
            cursor.execute("""SELECT * FROM acquisitionData WHERE start_date >=? AND end_date <= ?""", (startTime, endTime))
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
    def __init__(self):  
        super().__init__()  
    
    # Set endpoint, initialize SPARQLWrapper object query, send it and convert the result, create a dynamical dataframe
    def getAllPeople(self) -> pd.DataFrame:         # getting every information from the JSON file using one-line for-loops
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
        try:
            result_df = pd.DataFrame({"Name": pd.Series([row["name"]["value"] for row in result]), "Id": pd.Series([row["id"]["value"] for row in result]), 
                          "Uri": pd.Series([row["uri"]["value"]] for row in result)})
        except:
            result_df = pd.DataFrame(columns=["Name", "Id", "Uri"])
        return result_df

    # Same process  - in this case the query is more complex to return all the information
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
                ?uriAuthor <https://schema.org/givenName> ?nameAuthor . }
                OPTIONAL { ?uri <https://schema.org/datePublished> ?date . }
                 }
        """
        
        self.request.setQuery(self.query)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_rows = []                                   # since CHO can either have author/date or not, different cases have to be considered.

        for row in self.result:                                 # add each row of the DF built to match those information. For empty values empty
            if "nameAuthor" in list(row.keys()) and "date" in list(row.keys()):      # strings are used.
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
                                       "Id": pd.Series([row["id"]["value"]]), "Uri": pd.Series([row["uri"]["value"]]),
                                       "Author": pd.Series([""]), "Date Publishing": pd.Series([""]),
                                       "Place": pd.Series([row["namePlace"]["value"]]), "Owner": pd.Series([row["nameOwner"]["value"]])}))
        try:
            self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)     #concat all the precedents rows in a single df.
           
            dfs_to_concat = []
            for id, num in pd.pivot_table(self.result_df, index=["Id"], aggfunc="size").items(): # for each object that appears more than once
                if int(num) > 1:                                                                 # (this happens when there are more authors)
                    item_to_search = [id]

                    mask = self.result_df["Id"].isin(item_to_search) 
                    df_of_mult_authors = self.result_df[mask]                 #bring me a selection of the df in which it appears

                    all_auth_of_curr_obj = df_of_mult_authors["Author"].tolist()  #list of the authors
                    rows_to_drop = df_of_mult_authors.index.values.tolist()      #list of the indexes

                    auth = "; ".join(all_auth_of_curr_obj)                      #create one string for the author
                    row_to_insert = self.result_df.loc[[rows_to_drop[0]]].copy()                          #take one of the rows and copy it

                    row_to_insert.at[rows_to_drop[0],"Author"] = auth               #modify the authors putting in the single string with all of them

                    dfs_to_concat.append(row_to_insert)                            #we'll concateate that row
                    self.result_df = self.result_df.drop(x for x in rows_to_drop)      #drop all the rows with that id that are now useless

            dfs_to_concat.append(self.result_df)                             #add the original df with the deleted rows in the list that will be concatenated
            result = pd.concat(dfs_to_concat, join="outer", ignore_index=True)   #concatenate all the rows with the df
        except:
            result = pd.DataFrame(columns=["Object", "Type", "Id", "Uri", "Author", "Date Publishing", "Place", "Owner"])

        return result
    
    # Same process as getAllPeople but this time, use the f-string to insert dinamically the object to seach
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
        try:
            for row in self.result:
                self.result_rows.append(pd.DataFrame({"Name": pd.Series([row["name"]["value"]]), "Id": pd.Series([row["id"]["value"]]), 
                          "Uri": pd.Series([row["uri"]["value"]])}))
            self.result_df = pd.concat(self.result_rows, join="outer", ignore_index=True)

        except:
            self.result_df = pd.DataFrame(columns=["Name", "Id", "Uri"])

        return self.result_df

    # Again, same process.
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
        try:
            for row in self.result:                   # in this case the authors has to be present, so just the case of the missing date has to be considered.
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
    def row_to_obj(cls, s: pd.Series, 
                   use_case: str, 
                   cache_d: dict = {}, 
                   mode: str = "dict") -> Activity | IdentifiableEntity:
        """
        Initializes and returns an IdentifiableEntity or an Activity object based on the data contained in a 
        Pandas Series.
    
        Args:
            s: pd.Series = input data.

            use_case: str = determines the target of the method.\n
                - "act": indicates that the data in s has to be used to initialize
                         an Activity object.\n
                - "ch_obj": indicates that the data in s has to be used to initialize
                         a CulturalHeritage object.\n
                - "pers": indicates that the data in s has to be used to initialize
                         a Person object.\n
            \n
            cache_d: dict (default = {}) = dicitionary storing IdentifiableEntity objects using their id 
                                          as key. A custom one can be passed; otherwise 
                                          it will work as a cache.\n
            \n
            mode: str (default = "dict") = determines the behaviour of the function.\n
                - "dict": solely rely on the data provided in cache_d.\n
                - "in_row": use values in 's' to initialize a CulturalHeritageObject object.\n
                                
    
        Returns: An object of the class selected through the use_case parameter.
        """

        if use_case == "act":

            if s["internal_id"] in cache_d:
                return cache_d.get(s["internal_id"])

            if mode == "in_row" and s["object_id"] not in cache_d:
                obj = cls.row_to_obj(s, use_case="ch_obj", cache_d=cache_d)
                cache_d[s["object_id"]] = obj

            cult_obj = cache_d.get(s["object_id"])
            curr_type = s["type"]
            match curr_type: 
                case "acquisition":
                    obj = Acquisition(institute=s["responsible_institute"], 
                                    technique=s["technique"], 
                                    person=s["responsible_person"], 
                                    tool=s["tool"], 
                                    start=s["start_date"], 
                                    end=s["end_date"], 
                                    refersTo=cult_obj)
                case "processing":
                    obj = Processing(institute=s["responsible_institute"], 
                                    person=s["responsible_person"], 
                                    tool=s["tool"], 
                                    start=s["start_date"], 
                                    end=s["end_date"], 
                                    refersTo=cult_obj)                                        
                case "modelling":
                    obj = Modelling(institute=s["responsible_institute"], 
                                    person=s["responsible_person"], 
                                    tool=s["tool"], 
                                    start=s["start_date"], 
                                    end=s["end_date"], 
                                    refersTo=cult_obj)
                case "optimising":
                    obj = Optimising(institute=s["responsible_institute"], 
                                    person=s["responsible_person"], 
                                    tool=s["tool"], 
                                    start=s["start_date"], 
                                    end=s["end_date"], 
                                    refersTo=cult_obj)                                        
                case "exporting":
                    obj = Exporting(institute=s["responsible_institute"], 
                                    person=s["responsible_person"], 
                                    tool=s["tool"], 
                                    start=s["start_date"], 
                                    end=s["end_date"], 
                                    refersTo=cult_obj)
            
            cache_d[s["internal_id"]] = obj
            return obj
        
        elif use_case == "ch_obj": # if the function is passed to generate CHOs

            if s["Id"] in cache_d:
                return cache_d.get(s["Id"])
            object_type = s['Type']
            match object_type:
                case "Nautical chart":
                    obj = NauticalChart(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Printed volume":
                    obj = PrintedVolume(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Herbarium":
                    obj = Herbarium(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Printed material":
                    obj = PrintedMaterial(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Specimen":
                    obj = Specimen(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Painting":
                    obj = Painting(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Map":
                    obj = Map(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Manuscript volume":
                    obj = ManuscriptVolume(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Manuscript plate":
                    obj = ManuscriptPlate(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))
                case "Model":
                    obj = Model(id=str(s["Id"]),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=cls.getAuthorsOfCulturalHeritageObject(s["Id"]))     
                
            cache_d[s['Id']] = obj
            return obj
        
        elif use_case == "pers":

            if s['Id'] in cache_d:
                return cache_d.get(s['Id'])
            obj = Person(s['Id'], s['Name'])
            cache_d[s['Id']] = obj
            return obj
        
        else:
            raise TypeError("'use_case' parameter can only be 'act', 'ch_obj' and 'pers'")



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
        
    #@print_attributes
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
            for idx, s in df.iterrows():
                if "Object" in s.index:
                    match s["Type"]:
                        case "Nautical chart":
                            return NauticalChart(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Printed volume":
                            return PrintedVolume(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Herbarium":
                            return Herbarium(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Printed material":
                            return PrintedMaterial(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Specimen":
                            return Specimen(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Painting":
                            return Painting(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Map":
                            return Map(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Manuscript volume":
                            return ManuscriptVolume(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Manuscript plate":
                            return ManuscriptPlate(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                        case "Model":
                            return Model(id=str(id),title=s['Object'],date=str(s['Date Publishing']),owner=s['Owner'],place=s['Place'],hasAuthor=self.getAuthorsOfCulturalHeritageObject(s["Id"]))
                    
                else:
                    return Person(id,s["Person"])
            
        else:
            return None 
        
    #@print_attributes
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
    
    #@print_attributes
    def getAllCulturalHeritageObjects(self) -> list[CulturalHeritageObject]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getAllCulturalHeritageObjects()
            df_list.append(df_got)

        culturalHeritageObject_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Type' in row:
                    obj = None
                    object_type = row['Type']
                    if object_type == "Nautical chart":
                        obj = NauticalChart(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Printed volume":
                        obj = PrintedVolume(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Herbarium":
                        obj = Herbarium(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Printed material":
                        obj = PrintedMaterial(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Specimen":
                        obj = Specimen(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Painting":
                        obj = Painting(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Map":
                        obj = Map(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Manuscript volume":
                        obj = ManuscriptVolume(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Manuscript plate":
                        obj = ManuscriptPlate(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Model":
                        obj = Model(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    
                    if obj:
                        culturalHeritageObject_list.append(obj)
 
        return culturalHeritageObject_list
    
    #@print_attributes
    def getAuthorsOfCulturalHeritageObject(self, id: str) -> list[Person]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getAuthorsOfCulturalHeritageObject(id)
            df_got.drop_duplicates(inplace=True)
            df_list.append(df_got)
        
        

        person_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Name' in row and 'Id' in row:
                    person_list.append(Person(row['Id'],row['Name']))

        return person_list
    
    #@print_attributes
    def getCulturalHeritageObjectsAuthoredBy(self, id: str) -> list[CulturalHeritageObject]:
        df_list =[]
        for handler in self.metadataQuery:
            df_got = handler.getCulturalHeritageObjectsAuthoredBy(id)
            df_list.append(df_got)        

        culturalHeritageObject_list = []
        for df in df_list:
            for index, row in df.iterrows():
                if 'Type' in row:
                    obj = None
                    object_type = row['Type']
                    if object_type == "Nautical chart":
                        obj = NauticalChart(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Printed volume":
                        obj = PrintedVolume(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Herbarium":
                        obj = Herbarium(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Printed material":
                        obj = PrintedMaterial(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Specimen":
                        obj = Specimen(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Painting":
                        obj = Painting(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Map":
                        obj = Map(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Manuscript volume":
                        obj = ManuscriptVolume(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Manuscript plate":
                        obj = ManuscriptPlate(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    elif object_type == "Model":
                        obj = Model(id=row['Id'], title=row['Object'], date=row['Date Publishing'], owner=row['Owner'], place=row['Place'], hasAuthor=self.getAuthorsOfCulturalHeritageObject(row["Id"]))
                    
                    if obj:
                        culturalHeritageObject_list.append(obj)
 
        return culturalHeritageObject_list

    #@print_attributes
    def getAllActivities(self) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getAllActivities() for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list()

    #@print_attributes
    def getActivitiesByResponsibleInstitution(self, partialName: str) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getActivitiesByResponsibleInstitution(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list() 
    
    #@print_attributes
    def getActivitiesByResponsiblePerson(self, partialName: str) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getActivitiesByResponsiblePerson(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    #@print_attributes
    def getActivitiesUsingTool(self, partialName: str) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getActivitiesUsingTool(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    #@print_attributes
    def getActivitiesStartedAfter(self, date: str) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getActivitiesStartedAfter(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    #@print_attributes
    def getActivitiesEndedBefore(self, date: str) -> list[Activity]:
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getActivitiesEndedBefore(date) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Creating a dicitonary with all the CulturalHeritageObject objects based on their id
        unique_ids = final_df["object_id"].unique()
        cult_obj_dict = {obj_id:self.getEntityById(obj_id) for obj_id in unique_ids}
        # Reducing each row to a Series using apply, passing the objects dictionary to initialize the Activity objects
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", cache_d=cult_obj_dict, mode="dict"), axis=1, result_type="reduce")
        return obj_series.to_list()
    
    #@print_attributes
    def getAcquisitionsByTechnique(self, partialName: str) -> list[Acquisition]:
        result = [] # list to store the Activities objects
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        df_list = [handler.getAcquisitionsByTechnique(partialName) for handler in self.processdataQuery]
        final_df = pd.concat(df_list, ignore_index=True) # concatenating them
        if len(self.processdataQuery) > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Iterating over each row to initialize Acquisition objects
        for _, row in final_df.iterrows():
            cult_obj = self.getEntityById(row["object_id"])
            obj = Acquisition(
                            institute=row["responsible_institute"], 
                            technique=row["technique"], 
                            person=row["responsible_person"], 
                            tool=row["tool"], 
                            start=row["start_date"], 
                            end=row["end_date"], 
                            refersTo=cult_obj) # initializing object
            result.append(obj) # appending each object to result list and returning it
        
        return result

class AdvancedMashup(BasicMashup): 

    def __init__(self) -> None:
        super().__init__()

    #@print_attributes
    def getActivitiesOnObjectsAuthoredBy(self, personId: str): 
        # Checking if the list attributes are non-empty
        if (len_pq := len(self.processdataQuery)) == 0:
            raise AttributeError("No ProcessdataQueryHandler was specified for the AdvancedMashup process. Please add at least one")
        if (len_mq := len(self.metadataQuery)) == 0:
            raise AttributeError("No MetadataQueryHandler was specified for the AdvancedMashup process. Please add at least one")
        # Creating lists of Dataframes by appling a QueryHandler method to each QueryHandler object in the list
        mdf_list = [m_handler.getCulturalHeritageObjectsAuthoredBy(personId) for m_handler in self.metadataQuery]
        pdf_list = [p_handler.getAllActivities() for p_handler in self.processdataQuery]
        # Concatenating the two lists of dataframes
        m_conc_df = pd.concat(mdf_list, ignore_index=True) 
        p_conc_df = pd.concat(pdf_list, ignore_index=True)
        if p_conc_df.empty or m_conc_df.empty:
            return []
        # Merging the two concatenated Dataframes
        final_df = p_conc_df.merge(m_conc_df, how="right", left_on="object_id", right_on="Id")
        if len_mq > 1 or len_pq > 1:
            final_df.drop_duplicates(inplace=True, ignore_index=True)
        # Applying class method reducing Dataframe to Series containing objects instances, initialized based on the data in each row-Series
        obj_series = final_df.apply(lambda row: self.row_to_obj(row, use_case="act", mode="in_row"), axis=1, result_type="reduce")
        return obj_series.to_list() # returning the Series as a list 
        
    #@print_attributes
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
    
    #@print_attributes
    def getObjectsHandledByResponsibleInstitution(self, partialName: str):
        try:
            if len(self.processdataQuery) == 0:                                                  # checking if there are any PDQHs in the attribute 
                raise AttributeError("No MetadataQueryHandler set for the AdvancedMashup process. Please add at least one")
            if len(self.metadataQuery) == 0:                                                      # same but for the MDQHs
                raise AttributeError("No MetadataQueryHandler set for the AdvancedMashup process. Please add at least one")
            else:                                        # get the DFs using the corresponding methods using a quick in-line for loop
                pdf_list = [pd_handler.getActivitiesByResponsibleInstitution(partialName) for pd_handler in self.processdataQuery]
                mdf_list = [md_handler.getAllCulturalHeritageObjects() for md_handler in self.metadataQuery]

                p_concat_df = pd.concat(pdf_list, join="outer", ignore_index=True)              
                m_concat_df = pd.concat(mdf_list, join="outer", ignore_index=True)
                merged_df = pd.merge(p_concat_df, m_concat_df, left_on="object_id", right_on="Id")   # after concatenating the list of DFs, they are merged on the ID of the object

                parsed_obj =set()
                list_of_objs = []
                for index, row in merged_df.iterrows():   # this for loop is to check if there are duplicate of object in the df
                    if row["Id"] in parsed_obj:
                        pass
                    else:
                        list_of_objs.append(BasicMashup.row_to_obj(row, use_case="ch_obj")) #apply the class method to newly parsed CHOs to the list that will be returned.
                        parsed_obj.add(row["Id"])

                return list_of_objs # convert the series to a list.
                
        except Exception as e:
          return f"{e}"


    #@print_attributes
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