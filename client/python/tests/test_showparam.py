from datacat import client_from_config, config_from_file

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'

    config = config_from_file(config_file)
    client = client_from_config(config)

    #Case 1 Valid query and ignoreShowKeyError flag is present
    try:
        client.search(target='/testpath/testfolder', show='nIsTest', ignoreShowKeyError=True)
        print("Query passed as expected (Case 1: valid query and ignoreShowKeyError flag is present)")
    except:
        assert False, "Error. Query should be valid (Case 1)."

    #Case 2 Valid query and ignoreShowKeyError flag is not present
    try:
        client.search(target='/testpath/testfolder', show='nIsTest', ignoreShowKeyError=False)
        assert False, "Error. Query should be valid (Case 2)."
    except:
        print("Query passed as expected (Case 2: Valid query and ignoreShowKeyError flag is not present)")

    #Case 3 Invalid query and ignoreShowKeyError flag is present
    try:
        client.search(target='/testpath/testfolder', show='FakeKey', ignoreShowKeyError=True)
        print("Query passed as expected (Case 3: Invalid query and ignoreShowKeyError flag is present)")
    except:
        assert False, "Error. Query should be valid (Case 3)."

    #Case 4 Invalid query and ignoreShowKeyError flag is not present
    try:
        client.search(target='/testpath/testfolder', show='FakeKey', ignoreShowKeyError=False)
        assert False, "Error. Query should had failed (Case 4)."
    except:
        print("Query failed as expected (Case 4: Invalid query and ignoreShowKeyError flag is not present).")

    #Case 5 * query
    try:
        for dataset in client.search(target='/testpath/testfolder', show='*', ignoreShowKeyError=True):
            if hasattr(dataset, "metadata"):
                print("Dataset metadata: %s" %(dataset.metadata))
            else:
                print("Dataset: %s" %(dataset.name))

        print("Query passed as expected (Case 5: * query)")
    except:
        assert False, "Error. All metadata keys retrieval unsuccessful."


