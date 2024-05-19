import pandas as pd
from sqlite3 import connect, OperationalError
import json
import rdflib as rdf
from rdflib.namespace import SDO, RDF, RDFS
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import SPARQLWrapper as sw
from datetime import datetime

class Handler():
    
    dbPathOrURL = ""

    def getDbPathOrURL(self):
        return self.dbPathOrURL
    
    def setDbPathOrUrl(self, pathOrURL: str) -> bool:
        try:
            self.dbPathOrURL = pathOrURL
            print("Database location succesfully updated")
            return True
        except ValueError as e:
            print(f"{e}: input argument must be a string")
            return False
        except TypeError:
            print("Please specify a path or URL")


class ProcessDataQueryHandler(Handler):

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


# test -> getAllActivities
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getAllActivities())


# test -> getActivitiesByResponsibleInstitution
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getActivitiesByResponsibleInstitution("Co"))

# test -> getActivitiesByResponsiblePerson
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getActivitiesByResponsiblePerson("Ja"))

# test -> getActivitiesByResponsiblePerson
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getActivitiesUsingTool("Blender"))

# test -> getActivitiesStartedAfter
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getActivitiesStartedAfter("2023-06-23"))

# test -> getActivitiesEndedBefore
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getActivitiesEndedBefore("2023-06-23"))

# test -> getAcquisitionsByTechnique
# allActivities = ProcessDataQueryHandler()
# print(allActivities.dbPathOrURL) 
# allActivities.setDbPathOrUrl("databases/relational.db")
# print(allActivities.dbPathOrURL) 
# print(allActivities.getAcquisitionsByTechnique("Structured"))
