import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'

    config = config_from_file(config_file)
    client = client_from_config(config)

    #Case 1 Valid query and ignoreShowKeyError flag is present
    try:
        client.search(target='/testpath', show='nIsTest', ignoreShowKeyError=True)
        print("Query passed as expected (Case 1: valid query and ignoreShowKeyError flag is present)")
    except:
        print("Error. Query should be valid (Case 1).")

    #Case 2 Valid query and ignoreShowKeyError flag is not present
    try:
        client.search(target='/testpath', show='nIsTest', ignoreShowKeyError=False)
        print("Query passed as expected (Case 2: Valid query and ignoreShowKeyError flag is not present)")
    except:
        print("Error. Query should be valid (Case 2).")

    #Case 3 Invalid query and ignoreShowKeyError flag is present
    try:
        client.search(target='/testpath', show='FakeKey', ignoreShowKeyError=True)
        print("Query passed as expected (Case 3: Invalid query and ignoreShowKeyError flag is present)")
    except:
        print("Error. Query should be valid (Case 3).")

    #Case 4 Invalid query and ignoreShowKeyError flag is not present
    try:
        client.search(target='/testpath', show='FakeKey', ignoreShowKeyError=False)
        print("Query passed as expected (Case 4: Invalid query and ignoreShowKeyError flag is not present)")
        print("Error. Query should had failed.")
    except:
        print("Query failed as expected (Case 4: Invalid query and ignoreShowKeyError flag is not present).")

    #Case 5 * query
    try:
        for dataset in client.search(target='/testpath', show='*', ignoreShowKeyError=True):
            print("Dataset metadata: %s" %(dataset.metadata))

        print("Query passed as expected (Case 5: * query)")
    except:
        print("Error. All metadata keys retrieval unsuccessful.")





