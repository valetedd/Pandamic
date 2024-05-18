import unittest
from os import sep
from pandas import DataFrame
from impl import MetadataUploadHandler, ProcessDataUploadHandler, QueryHandler
from impl import MetadataQueryHandler, ProcessDataQueryHandler
from impl import AdvancedMashup
from impl import Person, CulturalHeritageObject, Activity, Acquisition
import pandas as pd
from sqlite3 import connect, OperationalError, OperationalError
import json
from util import *
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw
from urllib.parse import quote, urlencode, quote_plus
import datetime
import json
from pprint import pprint
'''def pushDataToDb(path):
 # if you try this, remember to update the endpoint depending on the one set when running blzgraph


        md_Series = pd.read_csv(path, keep_default_na=False, dtype={
            "Id":"string","Type":"string","Title":"string","Date":"string","Author":"string","Owner":"string","Place":"string"
        })
        md_Series= md_Series.rename(columns={"Id":"identifier","Type":"type","Title":"name","Date":"datePublished","Author":"author","Owner":"maintainer","Place":"spatial"})
        
        #pivot = pd.pivot_table(md_Series, index=["type"], aggfunc="size")
        #for idx, row in pivot.items():
        #    print(idx, "--", row)

        pv = "Printed volume"
        correct_stuff = [pv]
        mask = md_Series["type"].isin(correct_stuff)
        df = md_Series[mask]
        v_to_drop = df.index.values.tolist()

        for r in v_to_drop:
            md_Series = md_Series.drop(r)
        
        md_Series.at[0, "author"] = "peepee (uh)"
        print(md_Series["author"])
        
        # print (md_Series.iloc[[0]])

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
                    pass
                else:
                    if pred=="author":
                        if len(obj)==0:
                                pass
                        else:

                            list_of_auth = []
                            if ";" in obj:
                                # the_authors = obj.split
                                list_of_auth.extend(obj.split("; "))
                            else:
                                list_of_auth.append(obj)
                            # print (list_of_auth)
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
                                # print (row["name"], "has author:", name, "(", author_ID, ")")

pushDataToDb("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/data/meta.csv")
'''

#mduh = MetadataUploadHandler()
mdqh = MetadataQueryHandler()
mdqh.setDbPathOrUrl(pathOrURL="http://192.168.1.20:9999/blazegraph/sparql")
# pdqh = ProcessDataQueryHandler()
# pdqh.setDbPathOrUrl("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/databases/relational.db")
#mduh.pushDataToDb("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/data/meta.csv")
# mdqh.setDbPathOrUrl("http://10.201.47.161:9999/blazegraph/sparql")
# mdqh.getAllCulturalHeritageObjects()

print(mdqh.getCulturalHeritageObjectsAuthoredBy("pipo"))



'''
am = AdvancedMashup()
am.addMetadataHandler(mdqh)
am.addProcessHandler(pdqh)
test = am.getObjectsHandledByResponsibleInstitution("Heritage")
for obj in test:
    print(obj.getTitle())'''
'''
qh = QueryHandler()
qh.setDbPathOrUrl("http://192.168.1.57:9999/blazegraph/sparql")
test = qh.getById("ULAN:500114874")
print(test)
'''

# test2 = MetadataQueryHandler()
# test1 = MetadataQueryHandler()
# test1.setDbPathOrUrl("cazzusboia")
# test2.setDbPathOrUrl("ma chi cazzo è paolo bitta")
# print(test1.getDbPathOrURL())