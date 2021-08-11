import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testfolder'

    # ********** DATASET CREATION STARTS HERE **********
    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    filename = "dataset001.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file001,
                        site='SLAC')
    print("\ncreated dataset: ", filename)

    filename = "dataset002.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file002 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file002,
                        site='SLAC')
    print("\ncreated dataset: ", filename)

    filename = "dataset003.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file003= file_path + '/' + filename
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file003,
                        site='SLAC')
    print("\ncreated dataset: ", filename, "\n")

    did001 = client.get_dependency_id(ds001)
    did002 = client.get_dependency_id(ds002)
    did003 = client.get_dependency_id(ds003)
    vmetadata = { "dependency": did003,
                  "dependencyName": "test_data",
                  "dependents": [did001, did002],
                  "dependentType": "predecessor" }
    client.mkdependency(datacat_path+'/'+filename, metadata=vmetadata)
    dependents = client.get_dependents(did003, target="/testpath/**")

    # ********** CLIENT DEPENDENCY TESTING BEGINS HERE **********
    # Case 1.1: base case (predecessors) with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s \nDependents: " %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.1"

    # Case 1.2: base case (predecessors) with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents in (2)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.2"

    # Case 1.3: base case (predecessors) with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents in (2,4,5)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 1.3"

    # Case 2.1: predecessor with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessors", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.1"

    # Case 2.2: predecessor with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents.predecessors in (2)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.2"

    # Case 2.3: predecessor with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents.predecessors in (2,4,5)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 2.3"

    # Case 3.1: successor with versionPK value not specified
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successors", ignoreShowKeyError=True):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.1 "

    # Case 3.2: successor with one versionPK value specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents.successors in (2)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.2"

    # Case 3.3: successor with multiple versionPK values specified
    try:
        for dataset in client.search(target='/testpath/testfolder', query="dependents.successors in (2,4,5)"):
            print("\n***Dataset*** \nName: %s" %(dataset.name), "\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.3"