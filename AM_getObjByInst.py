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
mdqh.setDbPathOrUrl("http://10.201.57.198:9999/blazegraph/sparql")

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
                pprint(mdf_list)
                p_concat_df = pd.concat(pdf_list, join="outer", ignore_index=True)
                m_concat_df = pd.concat(mdf_list, join="outer", ignore_index=True)
                final_df = pd.merge(p_concat_df, m_concat_df, left_on="8", right_on="Id")
                

            
       # except:
        #    return False
        
AdvancedMashup().getObjectsHandledByResponsibleInstitution(partialName="Philology")