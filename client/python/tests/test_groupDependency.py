import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testFolder'

    containerPathSuc = "/testpath/depGroupSuc"
    containerPathPre = "/testpath/depGroupPre"

    containerPath1 = "/testpath/depGroup1"
    containerPath2 = "/testpath/depGroup2"

    # metadata
    metadata = Metadata()
    ds_metadata = Metadata()

    # deleting any duplicate groups
    try:
        if client.exists(containerPathSuc):
            client.rmdir(containerPathSuc, type="group")

        if client.exists(containerPathPre):
            client.rmdir(containerPathPre, type="group")

        if client.exists(containerPath1):
            client.rmdir(containerPath1, type="group")

        if client.exists(containerPath2):
            client.rmdir(containerPath2, type="group")

    except:
        print("exception caught here")

    # ********** DEPENDENCY GROUP DATASETS CREATION STARTS HERE ***************
    # *************************************************************************

    #dataset001
    filename = "dataset001_82f24.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)


    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=ds_metadata,
                        resource=full_file001,
                        site='SLAC')
    ds001VersionPk = ds001.versionPk
    ds001VersionID = ds001.versionId
    print("\nCreated dataset:\n{}\n{}\nMetadata: {}".format(filename, ds001, dict(ds_metadata)))

    #dataset002
    filename = "dataset002_92e56.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)


    full_file002 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=ds_metadata,
                        resource=full_file002,
                        site='SLAC')
    ds002VersionPk = ds002.versionPk
    print("\nCreated dataset:\n{}\n{}\nMetadata: {}".format(filename, ds002, dict(ds_metadata)))

    #dataset003
    filename = "dataset003_0c89c.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)


    full_file003 = file_path + '/' + filename
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=ds_metadata,
                        resource=full_file003,
                        site='SLAC')
    ds003VersionPk = ds003.versionPk
    print("\nCreated dataset:\n{}\n{}\nMetadata: {}".format(filename, ds003, dict(ds_metadata)))

    #dataset004
    filename = "dataset004_d8080.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    full_file004 = file_path + '/' + filename
    ds004 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=ds_metadata,
                        resource=full_file004,
                        site='SLAC')
    ds004VersionPk = ds004.versionPk
    print("\nCreated dataset:\n{}\n{}\nMetadata: {}".format(filename, ds004, dict(ds_metadata)))

    # Putting together a list to be used as predecessor dependents
    dependentsPredecessor = client.getdependentid([ds001,ds002])
    print("\ndependents genereated as:\n{}".format(dependentsPredecessor))

    # Putting together a list to be used as successor dependents
    dependentsSuccessor = client.getdependentid([ds003,ds004])
    print("\ndependents genereated as:\n{}".format(dependentsSuccessor))

    # Putting together a list of dependents that are located in multiple groups
    dependentsSameDatasetDifferentGroups = client.getdependentid([ds001,ds002])

    # adding the datasets with their new dependentType to a groups metadata (predecessor)
    dep_metadataPredecessor = {
        "dependents": str(dependentsPredecessor),
        "dependentType": "predecessor"
    }

    # adding the datasets with their new dependentType to a groups metadata (successor)
    dep_metadataSuccessor = {
        "dependents": str(dependentsSuccessor),
        "dependentType": "successor"
    }

    # adding the datasets with their new dependentType to a groups metadata (same datasets different groups)
    dep_metadataSameDatasetDifferentGroups = {
        "dependents": str(dependentsSameDatasetDifferentGroups),
        "dependentType": "predecessor"
    }

    metadata.update(dep_metadataPredecessor)
    depGroupPre = client.mkgroup(containerPathPre, metadata=metadata)
    print("\nCreated depGroupPre as:\n{} \nMetadata: {}".format(depGroupPre, dict(metadata)))

    metadata.update(dep_metadataSuccessor)
    depGroupSuc = client.mkgroup(containerPathSuc, metadata=metadata)
    print("\nCreated depGroupSuc as:\n{} \nMetadata: {}".format(depGroupSuc, dict(metadata)))

    metadata.update(dep_metadataSameDatasetDifferentGroups)
    depGroup1 = client.mkgroup(containerPath1, metadata=metadata)
    print("\nCreated depGroup1 as:\n{} \nMetadata: {}".format(dep_metadataSameDatasetDifferentGroups, dict(metadata)))

    metadata.update(dep_metadataSameDatasetDifferentGroups)
    depGroup2 = client.mkgroup(containerPath2, metadata=metadata)
    print("\nCreated depGroup2 as:\n{} \nMetadata: {}".format(dep_metadataSameDatasetDifferentGroups, dict(metadata)))

    # ********** CONTAINER DEPENDENCY TESTING BEGINS HERE **********
    # **************************************************************

    # Case 1.1: base case (predecessors) with versionPK value not specified
    # Retrieves all predecessor datasets linked to the group
    print("\n*****Case 1.1*****")
    print("-----Datasets-----")
    try:
        print(client.path(path='/testpath/depGroupPre;metadata=dependents'))
    except:
        assert False, "Error. search unsuccessful. Case 1.1"

    # Case 1.2: base case (predecessors) with one versionPK value specified
    # Retrieves specified predecessor datasets linked to the group
    print("\n*****Case 1.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupPre', show="dependents", query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.2"

    # Case 1.3: base case (predecessors) with multiple versionPK values specified
    # Retrieves all predecessor datasets linked to the group
    print("\n*****Case 1.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupPre', show="dependents", query='dependents in ({},{})'.format(ds001VersionPk,ds002VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.3"

    # Case 2.1: predecessor with versionPK value not specified
    # Retrieves all predecessor datasets linked to the group
    print("\n*****Case 2.1*****")
    print("-----Datasets-----")
    try:
        print(client.path(path='/testpath/depGroupPre;metadata=dependents.predecessor'))
    except:
        assert False, "Error. search unsuccessful. Case 2.1"

    # Case 2.2: predecessor with one versionPK value specified
    # Retrieves specified predecessor datasets linked to the group
    print("\n*****Case 2.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupPre', show="dependents.predecessor", query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.2"

    # Case 2.3: predecessor with multiple versionPK values specified
    # Retrieves specified predecessor datasets linked to the group
    print("\n*****Case 2.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupPre', show="dependents.predecessor", query='dependents in ({},{})'.format(ds001VersionPk, ds002VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.3"

    # Case 3.1: successor with versionPK value not specified
    # Retrieves all successor datasets linked to the group
    print("\n*****Case 3.1*****")
    print("-----Datasets-----")
    try:
        print(client.path(path='/testpath/depGroupSuc;metadata=dependents.successor'))
    except:
        assert False, "Error. search unsuccessful. Case 3.1"

    # Case 3.2: successor with one versionPK value specified
    # Retrieves specified successor datasets linked to the group
    print("\n*****Case 3.2*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupSuc', show="dependents.successor",query='dependents in ({})'.format(ds003VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 3.2"

    # Case 3.3: successor with multiple versionPK values specified
    # Retrieves specified successor datasets linked to the group
    print("\n*****Case 3.3*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/depGroupSuc', show="dependents.successor", query='dependents in ({},{})'.format(ds003VersionPk,ds004VersionPk), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 3.3\n"

    # Case 4: test dependents.groups  which should return the Groups associated with a dependent dataset.
    # Retrieves specified datasets and returns what groups are linked to it
    print("\n*****Case 4*****")

    print("-----Datasets-----")
    try:
        print(client.path(path='/testpath/testFolder/dataset001_82f24.dat;metadata=dependents.groups', versionId=0))
    except:
        assert False, "Error. search unsuccessful. Case 4"