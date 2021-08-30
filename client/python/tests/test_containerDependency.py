import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testfolder'

    containerPath1 = "/testpath/depGroup1"
    containerPath2 = "/testpath/depGroup2"

    # metadata
    metadata = Metadata()

    try:
        if client.exists(containerPath1):
            client.rmdir(containerPath1, type="group")

        if client.exists(containerPath2):
            client.rmdir(containerPath2, type="group")

    except:
        print("exception caught here")

    # Dependency Group 1 creation

    depGroup1 = client.mkgroup(containerPath1)
    print("\ncreated depGroup1 as: {}".format(depGroup1))

    # Dependency Group 2 creation



    # ********** DEPENDENCY GROUP DATASETS CREATION STARTS HERE ***************

    filename = "dataset001_82f24.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)


    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file001,
                        site='SLAC')
    ds001VersionPk = ds001.versionPk
    print("\ncreated dataset: ", filename)


    filename = "dataset002_92e56.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)


    full_file002 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file002,
                        site='SLAC')
    ds002VersionPk = ds002.versionPk
    print("\ncreated dataset: ", filename)


    dependents = client.getdependentid([ds001,ds002])
    print("\ndependents genereated as: {}".format(dependents))

    dep_metadata = {
        "dependents": str(dependents),
        "dependentType": "predecessor"
    }

    metadata.update(dep_metadata)
    depGroup2 = client.mkgroup(containerPath2, metadata=metadata)

    print("\ncreated depGroup2 as: {}".format(depGroup2))



    # ********** CONTAINER DEPENDENCY TESTING BEGINS HERE **********

    # Case 1.1: base case (predecessors) with versionPK value not specified

    print("\n*****Case 1.1*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.1"



    # Case 1.2: base case (predecessors) with one versionPK value specified

    print("\n*****Case 1.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents", query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.2"

    # Case 1.3: base case (predecessors) with multiple versionPK values specified

    print("\n*****Case 1.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents", query='dependents in ({},{})'.format(ds001VersionPk,ds002VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.3"

    # Case 2.1: predecessor with versionPK value not specified

    print("\n*****Case 2.1*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents.predecessor", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.1"

    # Case 2.2: predecessor with one versionPK value specified

    print("\n*****Case 2.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents.predecessor", query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.2"

    # Case 2.3: predecessor with multiple versionPK values specified

    print("\n*****Case 2.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup2', show="dependents.predecessor", query='dependents in ({},{})'.format(ds001VersionPk, ds002VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.3"

    # Case 3.1: successor with versionPK value not specified

    print("\n*****Case 3.1*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup1', show="dependents.successor", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 3.1"

    # Case 3.2: successor with one versionPK value specified

    print("\n*****Case 3.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup1', show="dependents.successor",query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 3.2"

    # Case 3.3: successor with multiple versionPK values specified

    print("\n*****Case 3.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroup1', show="dependents.successor", query='dependents in ({},{})'.format(ds001VersionPk,ds002VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
        print("\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.3\n"

