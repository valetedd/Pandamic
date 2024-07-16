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


# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:

# 1) Importing all the classes for handling the relational database
from impl import ProcessDataUploadHandler, ProcessDataQueryHandler

# 2) Importing all the classes for handling graph database
from impl import MetadataUploadHandler, MetadataQueryHandler

# 3) Importing the class for dealing with mashup queries
from impl import AdvancedMashup
# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "databases/relational.db"
# process = ProcessDataUploadHandler()
# process.setDbPathOrUrl(rel_path)
# process.pushDataToDb("data/process.json")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# Then, create the graph database (remember first to run the
# Blazegraph instance) using the related source data
blaz_url = "http://127.0.0.1:9999/blazegraph/" # copy-paste url appearing when the blazegraph instance is run
grp_endpoint =  blaz_url + "sparql"
# metadata = MetadataUploadHandler()
# metadata.setDbPathOrUrl(grp_endpoint)
# metadata.pushDataToDb("data/meta.csv")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# In the next passage, create the query handlers for both
# the databases, using the related classes
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl(grp_endpoint)

# Finally, create a advanced mashup object for asking
# about dataclear
mashup = AdvancedMashup()
mashup.addProcessHandler(process_qh)
# mashup.addProcessHandler(process_qh)
# mashup.addMetadataHandler(metadata_qh)
mashup.addMetadataHandler(metadata_qh)

# result_q1 = mashup.getEntityById("1")
# #print(f"method getEntityById, wrong input: {result_q1}\n")
# result_q2 = mashup.getEntityById("33")
# #print(f"method getEntityById Object: {result_q2}\n")
# result_q3 = mashup.getEntityById("VIAF:100190422")
# #print(f"method getEntityById author: {result_q3}\n")

# result_q4 = mashup.getAllPeople()
#print(f"method getAllPeople:{result_q4}\n")

# result_q5 = mashup.getAllCulturalHeritageObjects()
# print(f"method getAllCulturalHeritageObjets: {result_q5}\n")
# result_q6 = mashup.getAuthorsOfCulturalHeritageObject("17")
#print(f"method getAuthorsOfCulturalHeritageObject: {result_q6}")
# result_q7 = mashup.getAuthorsOfCulturalHeritageObject("45")
# #print(f"method getAuthorsOfCulturalHeritageObject wrong input: {result_q7}\n")

# result_q8 = mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:100190422")
# print(f"method getCulturalHeritageObjectsAuthoredBy: {result_q8}\n")
# result_q9 = mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:1")
#print(f"method getCulturalHeritageObjectsAuthoredBy wrong input: {result_q9}\n")

# result_q10 = mashup.getAllActivities()
# #print(f"method getAllActivities: {result_q10}\n")

# result_q11 = mashup.getActivitiesByResponsibleInstitution("Heritage")
# #print(f"method getActivitiesByResponsibleInstitution: {result_q11}\n")
# result_q12 = mashup.getActivitiesByResponsibleInstitution("Lidl")
# #print(f"method getActivitiesByResponsibleInstitution, wrong input: {result_q12} \n")

# result_q12 = mashup.getActivitiesByResponsiblePerson("Hopper")
# #print(f"method getActivitiesByResponsiblePerson: {result_q12}\n")
# result_q13 = mashup.getActivitiesByResponsiblePerson("Hitler")
# #print(f"method getActivitiesByResponsiblePerson, wrong input:{result_q13}\n")

# result_q14 = mashup.getActivitiesUsingTool("Nikon")
# #print(f"method getActivitiesUsingTool: {result_q14}\n")
# result_q15 = mashup.getActivitiesUsingTool("Zappa")
# #print(f"method getActivitiesUsingTool, wrong input: {result_q15}\n")

# result_q16 = mashup.getActivitiesStartedAfter("2023-08-21")
# #print(f"method getActivitiesStartedAfter: {result_q16}\n")
# result_q17 = mashup.getActivitiesStartedAfter("2028-01-01")
# #print(f"method getActivitiesStartedAfter, wrong input: {result_q17}\n")

# result_q18 = mashup.getActivitiesEndedBefore("2023-06-10")
# # #print(f"method getActivitiesEndedBefore: {result_q18}\n")
# result_q19 = mashup.getActivitiesEndedBefore("2028-01-01")
# # #print(f"method getActivitiesEndedBefore, wrong input: {result_q19}\n")

# result_q20 = mashup.getAcquisitionsByTechnique("Photogrammetry")
# #print(f"method getAcquisitionsByTechnique: {result_q20}\n")
# result_q21 = mashup.getAcquisitionsByTechnique("Zappa")
# #print(f"method getAcquisitionsByTechnique, wrong input: {result_q21}\n")

# #print("--- AdMash ---\n")

# result_q22 = mashup.getActivitiesOnObjectsAuthoredBy("VIAF:100190422")
# print(f"method getActivitiesOnObjectsAuthoredBy: {result_q22}\n")
# result_q23 = mashup.getActivitiesOnObjectsAuthoredBy("VIAF:1")
# #print(f"method getActivitiesOnObjectsAuthoredBy, wrong input: {result_q23}\n")

# result_q24 = mashup.getObjectsHandledByResponsiblePerson("Jane")
# #print(f"method getObjectsHandledByResponsiblePerson: {result_q24}\n")
# result_q25 = mashup.getObjectsHandledByResponsiblePerson("Chicoria")
# #print(f"method getObjectsHandledByResponsiblePerson, wrong input: {result_q25}\n")

# result_q26 = mashup.getObjectsHandledByResponsibleInstitution("Heritage")
# #print(f"method getObjectsHandledByResponsibleInstitution: {result_q26}\n")
# result_q27 = mashup.getObjectsHandledByResponsibleInstitution("Lidl")
# #print(f"method getObjectsHandledByResponsibleInstitution, wrong input: {result_q27}\n")

# result_q28 = mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-03-10", "2023-11-10")
# print(f"method getAuthorsOfObjectsAcquiredInTimeFrame: {result_q28}\n")
# result_q29 = mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2028-01-01", "2029-01-01")
# print(f"method getAuthorsOfObjectsAcquiredInTimeFrame, wrong input: {result_q29}\n")

activities = process_qh.getAllActivities()
# start_dates = sorted(activities['start_date'].unique())
# for date in start_dates:
#     if date:
#         df = process_qh.getActivitiesStartedAfter(date)
#         print(f"\n{date}\n\n{df}")
# date: 2023-12-10, expected activities: act-4c20c58, act-9eeb189
# print('\n-----------------------\n')
# end_dates = sorted(activities['end_date'].unique())
# print(end_dates)
# count = 0
# for date in end_dates:
#         df = process_qh.getActivitiesEndedBefore(date)
#         print(f"{date}\n\n{df}")
# lots of empty rows, only two expected activities: act-50d84b3, act-dc397d6