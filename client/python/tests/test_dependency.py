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
    datacat_path_dependent = '/testpath/dependents'
    datacat_path_general = '/testpath/general'

    # ********** DATASET CREATION STARTS HERE **********
    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    dp_metadata = Metadata()
    metadata['nIsTest'] = 1


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


    filename = "dataset003_0c89c.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    full_file003 = file_path + '/' + filename
    dependents = client.getdependentid([ds001, ds002])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "predecessor"}
    metadata.update(dep_metadata)
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file003,
                        site='SLAC')
    ds003VersionPk = ds003.versionPk
    ds003Dependency = ds003.versionMetadata["dependencyName"];
    print("\ncreated dataset: ", filename)


    filename = "dataset004_d8080.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    full_file004 = file_path + '/' + filename
    dependents = client.getdependentid([ds003])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "successor"}
    metadata.update(dep_metadata)
    ds004 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file004,
                        site='SLAC')
    print("\ncreated dataset: ", filename, "\n")

    # Creating scenario where dependent datasets and dataset being linked to are in different folders



    #create folders
    try:
        if client.exists(datacat_path_dependent):
            for dataset in client.search(target=datacat_path_dependent, show="dependents", ignoreShowKeyError=True):
                client.rmds(datacat_path_dependent + '/' + dataset.name)
            client.rmdir(datacat_path_dependent)

        if client.exists(datacat_path_general):
            for dataset in client.search(target=datacat_path_general, show="dependents", ignoreShowKeyError=True):
                client.rmds(datacat_path_general + '/' + dataset.name)
            client.rmdir(datacat_path_general)

    except:
        print("exception caught here")

    client.mkfolder(datacat_path_dependent)
    client.mkfolder(datacat_path_general)

    # in dependents folder
    filename = "dataset001_dp_b9253.dat"
    if client.exists(datacat_path_dependent + '/' + filename):
        client.rmds(datacat_path_dependent + '/' + filename)


    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path_dependent, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=dp_metadata,
                        resource=full_file001,
                        site='SLAC')
    ds001VersionPk_dp = ds001.versionPk
    print("\ncreated dataset: ", filename)



    filename = "dataset002_dp_43d4c.dat"
    if client.exists(datacat_path_dependent + '/' + filename):
        client.rmds(datacat_path_dependent + '/' + filename)


    full_file001 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path_dependent, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=dp_metadata,
                        resource=full_file001,
                        site='SLAC')
    ds002VersionPk_dp = ds002.versionPk
    print("\ncreated dataset: ", filename)


    # in general folder
    filename = "dataset001_gr_28e3d.dat"
    if client.exists(datacat_path_general + '/' + filename):
        client.rmds(datacat_path_general + '/' + filename)

    dependents = client.getdependentid([ds001, ds002])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "predecessor"}

    dp_metadata.update(dep_metadata)

    full_file001 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path_general, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=dp_metadata,
                        resource=full_file001,
                        site='SLAC')

    ds002Dependency = ds002.versionMetadata["dependencyName"];
    print("\ncreated dataset: ", filename)

    # ********** CLIENT DEPENDENCY TESTING BEGINS HERE **********
    # Case 1.1: base case (predecessors) with versionPK value not specified
    print("\n*****Case 1.1*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents", ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents",query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents",query='dependents in ({},{})'.format(ds001VersionPk, ds002VersionPk), ignoreShowKeyError=True):
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
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessor", ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents.predecessor",query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents.predecessor",query='dependents in ({},{})'.format(ds001VersionPk,ds002VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.3"

    # Case 3.1: successor with versionPK value not specified
    print("\n*****Case 3.1*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor", ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents.successor",query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
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
        for dataset in client.search(target=ds003Dependency, show="dependents.successor",query='dependents in ({},{})'.format(ds001VersionPk,ds002VersionPk), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
        print("\n")
    except:
        assert False, "Error. search unsuccessful. Case 3.3\n"


    # Case 1.3b: Getting datasets from dependents that are in DIFFERENT folders
    print("\n*****Case 1.3b*****")
    print("-----Datasets-----")
    try:
        for dataset in client.search(target=ds002Dependency, show="dependents",query='dependents in ({},{})'.format(ds001VersionPk_dp, ds002VersionPk_dp), ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.3b"
