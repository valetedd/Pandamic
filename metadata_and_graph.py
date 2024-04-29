# import impl as i
import sqlite3 as s3
import pandas as pd
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
import SPARQLWrapper as sw


class MetadataUploadHandler(): # (i.UploadHandler):
    def __init__(self):

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
            


        md_Series = pd.read_csv("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/DS/project/meta.csv", keep_default_na=False, dtype={
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

MetadataUploadHandler()