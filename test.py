# -*- coding: utf-8 -*-
# Copyright (c) 2023, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
import unittest
from os import sep
from pandas import DataFrame
from impl import MetadataUploadHandler, ProcessDataUploadHandler, Handler
from impl import MetadataQueryHandler, ProcessDataQueryHandler, QueryHandler
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

# REMEMBER: before launching the tests, please run the Blazegraph instance!
'''
class TestProjectBasic(unittest.TestCase):

    # The paths of the files used in the test should change depending on what you want to use
    # and the folder where they are. Instead, for the graph database, the URL to talk with
    # the SPARQL endpoint must be updated depending on how you launch it - currently, it is
    # specified the URL introduced during the course, which is the one used for a standard
    # launch of the database.
    metadata = "data" + sep + "meta.csv"
    process = "data" + sep + "process.json"
    relational = "." + sep + "relational.db"
    graph = "http://127.0.0.1:9999/blazegraph/sparql"
    
    def test_01_MetadataUploadHandler(self):
        u = MetadataUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.graph))
        self.assertEqual(u.getDbPathOrUrl(), self.graph)
        self.assertTrue(u.pushDataToDb(self.metadata))

    def test_02_ProcessDataUploadHandler(self):
        u = ProcessDataUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.relational))
        self.assertEqual(u.getDbPathOrUrl(), self.relational)
        self.assertTrue(u.pushDataToDb(self.process))
    
    def test_03_MetadataQueryHandler(self):
        q = MetadataQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.graph))
        self.assertEqual(q.getDbPathOrUrl(), self.graph)

        self.assertIsInstance(q.getById("just_a_test"), DataFrame)

        self.assertIsInstance(q.getAllPeople(), DataFrame)
        self.assertIsInstance(q.getAllCulturalHeritageObjects(), DataFrame)
        self.assertIsInstance(q.getAuthorsOfCulturalHeritageObject("just_a_test"), DataFrame)
        self.assertIsInstance(q.getCulturalHeritageObjectsAuthoredBy(
            "just_a_test"), DataFrame)
    
    def test_04_ProcessDataQueryHandler(self):
        q = ProcessDataQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.graph))
        self.assertEqual(q.getDbPathOrUrl(), self.graph)

        self.assertIsInstance(q.getById("just_a_test"), DataFrame)

        self.assertIsInstance(q.getAllActivities(), DataFrame)
        self.assertIsInstance(q.getActivitiesByResponsibleInstitution(
            "just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesByResponsiblePerson("just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesUsingTool("just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesStartedAfter("1088-01-01"), DataFrame)
        self.assertIsInstance(q.getActivitiesEndedBefore("2029-01-01"), DataFrame)
        self.assertIsInstance(q.getAcquisitionsByTechnique("just_a_test"), DataFrame)
        
    def test_05_AdvancedMashup(self):
        qm = MetadataQueryHandler()
        qm.setDbPathOrUrl(self.graph)
        qp = ProcessDataQueryHandler()
        qp.setDbPathOrUrl(self.relational)

        am = AdvancedMashup()
        self.assertIsInstance(am.cleanMetadataHandlers(), bool)
        self.assertIsInstance(am.cleanProcessHandlers(), bool)
        self.assertTrue(am.addMetadataHandler(qm))
        self.assertTrue(am.addProcessHandler(qp))

        self.assertEqual(am.getEntityById("just_a_test"), None)

        r = am.getAllPeople()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)

        r = am.getAllCulturalHeritageObjects()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAuthorsOfCulturalHeritageObject("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)

        r = am.getCulturalHeritageObjectsAuthoredBy("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAllActivities()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesByResponsibleInstitution("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesByResponsiblePerson("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesUsingTool("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesStartedAfter("1088-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesEndedBefore("2029-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getAcquisitionsByTechnique("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Acquisition)

        r = am.getActivitiesOnObjectsAuthoredBy("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getObjectsHandledByResponsiblePerson("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getObjectsHandledByResponsibleInstitution("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAuthorsOfObjectsAcquiredInTimeFrame("1088-01-01", "2029-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)   
'''
'''
def pushDataToDb(path):
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

mduh = MetadataUploadHandler()
mdqh = MetadataQueryHandler()
mdqh.setDbPathOrUrl(pathOrURL="http://192.168.1.57:9999/blazegraph/sparql")
pdqh = ProcessDataQueryHandler()
pdqh.setDbPathOrUrl("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/databases/relational.db")
# mduh.pushDataToDb("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/data/meta.csv")
# mdqh.setDbPathOrUrl("http://10.201.47.161:9999/blazegraph/sparql")
# mdqh.getAllCulturalHeritageObjects()

am = AdvancedMashup()
am.addMetadataHandler(mdqh)
am.addProcessHandler(pdqh)
test = am.getObjectsHandledByResponsibleInstitution("Heritage")
for obj in test:
    print(obj.getTitle())
