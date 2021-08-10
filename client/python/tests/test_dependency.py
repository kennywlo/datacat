import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'

    config = config_from_file(config_file)
    client = client_from_config(config)


    #Case 1.1: base case (predecessors) with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s \nDependents: " %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.1"

    #Case 1.2: base case (predecessors) with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents=[2]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.2"

    #Case 1.3: base case (predecessors) with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents=[2,4,5]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.3"



    #Case 2.1: predecessor with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessors", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.1"

    #Case 2.2: predecessor with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessors=[2]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.2"

    #Case 2.3: predecessor with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessors=[2,4,5]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.3"



    #Case 3.1: successor with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.1 "

    #Case 3.2: successor with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor=[2]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.2"

    #Case 3.3: successor with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor=[2,4,5]", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.3"