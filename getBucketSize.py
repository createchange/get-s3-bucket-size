import boto3
from pprint import pprint
import math
import json

client = boto3.client('s3')

def convert_size(size_bytes): 
    '''
    Conversation function found here: https://python-forum.io/Thread-Convert-file-sizes-will-this-produce-accurate-results
    '''
    if size_bytes == 0: 
        return "0B" 
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB") 
    i = int(math.floor(math.log(size_bytes, 1024)))
    power = math.pow(1024, i) 
    size = round(size_bytes / power, 2) 
    return "%s%s" % (size, size_name[i])

def getTopLevelObjects(client):
    '''
    Gets top level folders in the bucket and returns a list of their names.
    '''
    tlo = []
    response = client.list_objects_v2(Bucket="int-duplicity", Delimiter="/")
    for folder in response['CommonPrefixes']:
        for k,v in folder.items():
            tlo.append(v)
    return tlo

def getFolderSize(client,tlo):
    '''
    takes top level folder as name, which is used as 'Prefix' argument to get all child objects
    Returns the following in a dict:
        Folder: folder-name
        Size: size
    '''
    file_list = []
    obj_size_bytes = 0
    
    response = client.list_objects_v2(Bucket="int-duplicity", Prefix="%s" % tlo)

    total_obj = int(len(response['Contents']))
    for i in range(total_obj):
        obj_size_bytes += int(response['Contents'][i]['Size'])
        file_list.append({"File":response['Contents'][i]['Key'], "Size": response['Contents'][i]['Size']})
    obj_size_final = convert_size(obj_size_bytes)

    sorted_file_list = sorted(file_list, key=lambda k: k['Size'], reverse=True)
    for item in sorted_file_list:
        item['Size'] = str(convert_size(item['Size']))

    return {'Folder':response['Prefix'].strip('/'), 'Size':obj_size_bytes, 'SubObjects': sorted_file_list}

def sort_results(results):
    sorted_results = sorted(results, key=lambda k: k['Size'], reverse=True)
    for item in sorted_results:
        item['Size'] = str(convert_size(item['Size']))
    return(sorted_results)

def create_output_file(final_results):
    with open("bucketObjectInfo.json","w") as f:
        for result in final_results:
            json.dump(result,f, indent=4)
    

'''
Main
'''
results = []

tlo = getTopLevelObjects(client)
for folder in tlo:
    data = getFolderSize(client,folder)
    results.append(data)

final_results = sort_results(results)
create_output_file(final_results)
