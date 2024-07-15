from impl import *

m1 = MetadataUploadHandler()
#m1.setDbPathOrUrl(pathOrURL="http://192.168.149.123:9999/blazegraph/sparql")
#m1.pushDataToDb("data/meta.csv")
m2 = MetadataQueryHandler()
m2.setDbPathOrUrl("http://192.168.149.123:9999/blazegraph/sparql")
'''
for idx, row in m2.getAllCulturalHeritageObjects().iterrows():
    print (row["Object"], row["Date Publishing"])
    '''
p2 = ProcessDataQueryHandler()
p2.setDbPathOrUrl("databases/relational.db")
am = AdvancedMashup()
am.addMetadataHandler(m2)
am.addProcessHandler(p2)

for obj in am.getObjectsHandledByResponsibleInstitution("engineering"):
    print (obj.getTitle(), "has as PY:", obj.getDate())
