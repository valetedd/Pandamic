# import impl as i
import sqlite3 as s3
import pandas as pd
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
import SPARQLWrapper as sw

#we'll need to change some stuff (inherit the endpoint from the parent)

class MetadataQueryHandler():
    def __init__(self):   # Step 1. first of all, i set a fixed endpoint and format to return
        self.endpoint = "http://10.201.16.20:9999/blazegraph/sparql"
        self.request = sw.SPARQLWrapper(self.endpoint)
        self.request.setReturnFormat(sw.JSON)
    
    # Step 2. set query, send it and convert the result, create a dynamical dataframe getting every information from the JSON file using one-line for-loops
    def getAllPeople(self):
        self.request.setQuery("""
        SELECT ?name ?id ?uri
        WHERE { ?uri <https://schema.org/givenName> ?name ;
                     <https://schema.org/identifier> ?id . }
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_df = pd.DataFrame({"Name": pd.Series([row["name"]["value"] for row in self.result]), "Id": pd.Series([row["id"]["value"] for row in self.result]), 
                          "Uri": pd.Series([row["uri"]["value"]] for row in self.result)})
        print(self.result_df)

    # Step 3. do it again
    def getAllCulturalHeritageObject(self):
        self.request.setQuery("""
        SELECT ?obj ?type ?id ?uri
        WHERE { ?uri <https://schema.org/name> ?obj ;
                     rdf:type ?typeUri ;
                     <https://schema.org/identifier> ?id .
                ?typeUri rdfs:label ?type . }
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_df = pd.DataFrame({"Object": pd.Series([row["obj"]["value"] for row in self.result]), "Type": pd.Series([row["type"]["value"] for row in self.result]),
                                       "Id": pd.Series([row["id"]["value"] for row in self.result]), "Uri": pd.Series([row["uri"]["value"] for row in self.result])}) 
        print (self.result_df)
    
    # Step 4. do it again. But this time, use the f function to insert dinamically the object to seach
    def getAuthorsOfCulturalHeritageObject(self, objectId):
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
        print(self.result_df)
    
    # Step 5. someone stop me (I've done it again)
    def getCulturalHeritageObjectsAuthoredBy(self, personId):
        self.request.setQuery(f"""
        SELECT ?obj ?type ?id ?uri
        WHERE {{ ?uri <https://schema.org/name> ?obj ;
                     rdf:type ?typeUri ;
                     <https://schema.org/identifier> ?id ;
                     <https://schema.org/author> ?persUri .
                 ?typeUri rdfs:label ?type .
                 ?persUri <https://schema.org/givenName> '{personId}' . }}
        """)
        self.result = self.request.query().convert()
        self.result = self.result["results"]["bindings"]
        self.result_df = pd.DataFrame({"Object": pd.Series([row["obj"]["value"] for row in self.result]), "Type": pd.Series([row["type"]["value"] for row in self.result]),
                                       "Id": pd.Series([row["id"]["value"] for row in self.result]), "Uri": pd.Series([row["uri"]["value"] for row in self.result])}) 
        print (self.result_df)
