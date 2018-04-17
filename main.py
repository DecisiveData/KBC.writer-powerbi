import sys
import os
import csv
import json
import datetime
import httplib2
from keboola import docker

def truncate(workspace_id, dataset_id, table, token):
    #gen url
    url = "https://api.powerbi.com/v1.0/myorg"
    if workspace_id:
        url += "/groups/" + workspace_id
    #run truncate
    h = httplib2.Http(".cache")
    print("Truncate: " + url + "/datasets/" + dataset_id + "/tables/" + table + "/rows")
    (resp, content) = h.request(url + "/datasets/" + dataset_id + "/tables/" + table + "/rows",
                    "DELETE", 
                    headers = {
                        "content-type": "application/json",
                        "Authorization": "Bearer " + token
                    })
    if resp["status"] != "200":
        raise Exception('Error truncating table: ' + table + "\n\n" + str(content))

def upload(workspace_id, dataset_id, table, body, token):
    #gen url
    url = "https://api.powerbi.com/v1.0/myorg"
    if workspace_id:
        url += "/groups/" + workspace_id
    #run upload
    h = httplib2.Http(".cache")
    print("Uploading: " + url + "/datasets/" + dataset_id + "/tables/" + table + "/rows  (" + str(len(body)) + " bytes)")
    (resp, content) = h.request(url + "/datasets/" + dataset_id + "/tables/" + table + "/rows",
                    "POST", 
                    body = "{\"rows\":[" + body + "]}",
                    headers = {
                        "content-type": "application/json",
                        "Authorization": "Bearer " + token
                    })
    if resp["status"] != "200":
        raise Exception('Error uploading data into table: ' + table + "\n\n" + str(content))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
### Access the supplied config
cfg = docker.Config('/data/')
params = cfg.get_parameters()
### Set the batch size from params
batchSize = 9999
if params["batchSize"]:
    batchSize = int(params["batchSize"])

for filename in os.listdir(DEFAULT_FILE_INPUT):
    if filename.endswith(".csv"):
        filename_split = filename.split(".")
        ext = filename_split.pop()
        table = filename_split.pop()
        print("Processing Table: {0}".format(table))
        #with open(i, mode="rt") as in_file:
        with open(DEFAULT_FILE_INPUT+filename, mode="rt", encoding="utf-8") as in_file:
            rowNum = 0
            body = ""
            lazy_lines = (line.replace("\0", "") for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator="\n")
            #truncate the table first
            if params["truncate"]:
                truncate(params["workspace_id"], params["dataset_id"], table, params["token"])
            #batch add data back in
            for row in reader:
                if len(body) > 0:
                    body += ","
                #truncate any values that are > 4000 chars
                for key in row:
                    if len(str(row[key])) > 4000:
                        row[key] = str(row[key])[0:4000]
                body += json.dumps(row)
                rowNum += 1
                #upload in batches of 10k as per pbi api limits
                if rowNum == batchSize:
                    upload(params["workspace_id"], params["dataset_id"], table, body, params["token"])
                    rowNum = 0
                    body = ""
            #upload remaining data
            if len(body) > 0:
                upload(params["workspace_id"], params["dataset_id"], table, body, params["token"])

print("Done!")
