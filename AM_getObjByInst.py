import pandas as pd
from sqlite3 import connect, OperationalError
import json
from util import *
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw
from impl import *
from ProcessDataQueryHandler import ProcessDataQueryHandler
from pprint import pprint

pdqh = ProcessDataQueryHandler()
pdqh.setDbPathOrUrl("C:/Users/nicco/OneDrive/Desktop/DHDK/1st Year/courses/2nd semester/IMaWT/GitHub/Pandamic/databases/relational.db")
mdqh = MetadataQueryHandler()
mdqh.setDbPathOrUrl("http://192.168.1.20:9999/blazegraph/sparql")

class AdvancedMashup(BasicMashup):
    def __init__(self):
        self.processdataQuery = [pdqh]
        self.metadataQuery = [mdqh]
        

    def getObjectsHandledByResponsibleInstitution(self, partialName:str):
       #  try:
            if len(self.processdataQuery) == 0:
                print("No MetadataQueryHandler set for the mashup process. Please add at least one")
                return []
            if len(self.metadataQuery) == 0:
                print("No MetadataQueryHandler set for the mashup process. Please add at least one")
                return []
            else:
                pdf_list = [pd_handler.getActivitiesByResponsibleInstitution(partialName) for pd_handler in self.processdataQuery]
                mdf_list = [md_handler.getAllCulturalHeritageObjects() for md_handler in self.metadataQuery]
                p_concat_df = pd.concat(pdf_list, join="outer", ignore_index=True)
                m_concat_df = pd.concat(mdf_list, join="outer", ignore_index=True)
                final_df = pd.merge(p_concat_df, m_concat_df, left_on="8", right_on="Id")
                return final_df

            
       # except:
        #    return False

# prova a frammentare .query() e .convert(). altrimenti, prima inizializza la classe e poi chiama il metodo. Ma il problema ce l'aveva anche valentino, potrebbe essere legato al fatto che lui
# ha messo il valore dbPath dell'handler come comune a tutta la classe, fuori dall'__init__()? in ogni caso è quasi sicuramente un problema legato all'endpoint.
test = AdvancedMashup()
print(test.getObjectsHandledByResponsibleInstitution("Philology"))
