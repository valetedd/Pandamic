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
mdqh.setDbPathOrUrl("http://10.201.41.233:9999/blazegraph/sparql")

class AdvancedMashup(BasicMashup):
    def __init__(self):
        self.processdataQuery = [pdqh]
        self.metadataQuery = [mdqh]
        

    def getObjectsHandledByResponsibleInstitution(self, partialName:str):
        try:
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
                merged_df = pd.merge(p_concat_df, m_concat_df, left_on="object_id", right_on="Id")
                
                list_to_return = list()
                list_id = list()
                
                for idx, row in merged_df.iterrows():
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
                    
                    if len(list_to_return) == 0:
                        list_to_return.append(obj_to_append)
                        list_id.append(obj_to_append.getId())
                    else:   
                        if obj_to_append.getId() not in list_id:
                            list_to_return.append(obj_to_append)
                            list_id.append(obj_to_append.getId())
                        else:
                            pass

                return list_to_return
                
        except Exception as e:
          return f"{e}"


# test = AdvancedMashup().getObjectsHandledByResponsibleInstitution("Heritage")
# for t in test:
#   print(t.getAuthors())
