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

    # **************************************************
    # **************************************************
    # ********** DATASET CREATION STARTS HERE **********
    # **************************************************
    # **************************************************

    # *********************************************
    # ********** for PREDECESSOR testing **********
    # *********************************************

    # initializing metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1
    # initializing dependency metadata
    dp_metadata = Metadata()
    metadata['nIsTest'] = 1

    # creation of dataset001
    filename = "dataset001_82f24.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset001 - DOES NOT initialize dependency metadata
    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file001,
                        site='SLAC')
    ds001VersionPk = ds001.versionPk
    print("\ncreated dataset: ", filename, "(For Predecessor Testing) (VersionPK = ", ds001VersionPk,")")

    # creation of dataset002
    filename = "dataset002_92e56.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset002 - DOES NOT initialize dependency metadata
    full_file002 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file002,
                        site='SLAC')
    ds002VersionPk = ds002.versionPk
    print("\ncreated dataset: ", filename, "(For Predecessor Testing) (VersionPK = ", ds002VersionPk,")")

    # creation of dataset003
    filename = "dataset003_0c89c.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset003 - DOES initialize dependency metadata
    full_file003 = file_path + '/' + filename
    # Add Dependency metadata to dataset003 - Will list dataset001 and dataset002 as predecessors of dataset003
    dependents = client.get_dependent_id([ds001, ds002])
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
    print("\ncreated dataset: ", filename, "(For Predecessor Testing) (VersionPK = ", ds003VersionPk,")")

    # *******************************************
    # ********** For SUCCESSOR testing **********
    # *******************************************

    # reinitialization of metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    # creation of dataset004
    filename = "dataset004_d8080.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset004 - DOES not initialize dependency metadata
    full_file004 = file_path + '/' + filename
    ds004 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file004,
                        site='SLAC')
    ds004VersionPk = ds004.versionPk
    print("\ncreated dataset: ", filename, "(For Successor Testing) (VersionPK = ", ds004VersionPk,")")

    # creation of dataset005
    filename = "dataset005_62036.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset005 - DOES initialize contain dependency metadata
    full_file005 = file_path + '/' + filename
    ds005 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file005,
                        site='SLAC')
    ds005VersionPk = ds005.versionPk
    print("\ncreated dataset: ", filename, "(For Successor Testing) (VersionPK = ", ds005VersionPk,")")

    # creation of dataset006
    filename = "dataset006_07af1.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    # use the client to create dataset006 - DOES initialize dependency metadata
    full_file006 = file_path + '/' + filename
    # Add Dependency metadata to dataset006 - Will list dataset004 and dataset005 as successors of dataset003
    dependents = client.get_dependent_id([ds004, ds005])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "successor"}
    metadata.update(dep_metadata)
    ds006 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file006,
                        site='SLAC')
    ds006VersionPk = ds006.versionPk
    ds006Dependency = ds006.versionMetadata["dependencyName"];
    print("\ncreated dataset: ", filename, "(For Successor Testing) (VersionPK = ", ds006VersionPk,")")

    # **************************************************
    # ********** For DIFFERENT FOLDER testing **********
    # **************************************************
    # Different folder means that we expects datasets to be in different datacat folders but still linked by dependency

    metadata = Metadata()
    metadata['nIsTest'] = 1

    # Create 2 folders
    datacat_path_dependent = '/testpath/dependents'
    datacat_path_general = '/testpath/general'

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

    # Use the client to create the folders
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
    print("\ncreated dataset: ", filename, "(For Different Folder Testing) (VersionPK = ", ds001VersionPk_dp,")")


    filename = "dataset002_dp_43d4c.dat"
    if client.exists(datacat_path_dependent + '/' + filename):
        client.rmds(datacat_path_dependent + '/' + filename)


    full_file001 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path_dependent, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=dp_metadata,
                        resource=full_file001,
                        site='SLAC')
    ds002VersionPk_dp = ds002.versionPk
    print("\ncreated dataset: ", filename, "(For Different Folder Testing) (VersionPK = ", ds002VersionPk_dp,")")


    # in general folder
    filename = "dataset001_gr_28e3d.dat"
    if client.exists(datacat_path_general + '/' + filename):
        client.rmds(datacat_path_general + '/' + filename)

    dependents = client.get_dependent_id([ds001, ds002])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "predecessor"}

    dp_metadata.update(dep_metadata)

    full_file001 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path_general, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=dp_metadata,
                        resource=full_file001,
                        site='SLAC')

    ds002Dependency = ds002.versionMetadata["dependencyName"]
    ds001GeneralVersionPk = ds002.versionPk
    print("\ncreated dataset: ", filename, "(For Different Folder Testing) (VersionPK = ", ds001GeneralVersionPk,")")

    # ***************************************************
    # ********** For CUSTOM DEPENDENCY FIELD testing **********
    # ***************************************************

    metadata = Metadata()
    metadata['nIsTest'] = 1

    filename = "customDependency01.dat"
    # Creation of dataset customDependencyFile01
    if client.exists(datacat_path + "/" +filename):
        client.rmds(datacat_path + "/" +filename)
    # Use the client to create the dataset - DOES not initialize dependency metadata
    full_file_path = file_path + "/" + filename
    dsCustom001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                              versionMetadata=metadata,
                              resource=full_file_path,
                              site='SLAC')
    dsCustom001VersionPk = dsCustom001.versionPk
    print("\ncreated dataset: ", filename, "(For Custom Dependency Testing) (VersionPK = ", dsCustom001VersionPk,
          ")")

    filename = "customDependency02.dat"
    # Creation of dataset customDependencyFile02
    if client.exists(datacat_path + "/" + filename):
        client.rmds(datacat_path + "/" + filename)

    dependents = client.get_dependent_id([dsCustom001])
    dep_metadata = {"dependencyName": "test_data",
                    "dependents": str(dependents),
                    "dependentType": "analysis__XXX"}

    dp_metadata.update(dep_metadata)

    # Use the client to create the dataset - DOES initialize dependency metadata
    full_file_path02 = file_path + "/" + filename
    dsCustomField002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                                   versionMetadata=dep_metadata,
                                   resource=full_file_path02,
                                   site='SLAC')
    dsCustom2VersionPk = dsCustomField002.versionPk
    dsCustom2Dependency = dsCustomField002.versionMetadata["dependencyName"];
    print("\ncreated dataset: ", filename, "(For Custom Dependency Testing) (VersionPK = ", dsCustom2VersionPk,
          ")")

    # ***********************************************************
    # ***********************************************************
    # ********** CLIENT DEPENDENCY TESTING BEGINS HERE **********
    # ***********************************************************
    # ***********************************************************

    # (Case 1) base case (predecessors)
    # Test 1.1: Print all datasets found within a path alongside their predecessor dependency metadata.
    print("\n*****Case 1.1*****")
    print("-----------------------------------------------------------------------")
    print("Returns all datasets from the specified Target")
    print("Shows which ones contain dependents of the base case type (predecessor)")
    print("-----------------------------------------------------------------------")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 1.1"

    # Test 1.2: Return a single dependent by specifying the dependents versionPK and its parents dependency
    # --- Now the we know what datasets have what predecessor metadata, we can
    # retrieve specific dependents linked to a dataset by using the query parameter. ---
    print("\n*****Case 1.2*****")
    print("-------------------------------------------------------------")
    print("Returning datasets specified by a versionPks and a dependency")
    print("-------------------------------------------------------------")
    try:
        for dataset in client.search(target=ds003Dependency,show="dependents", query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Path: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 1.2"

    # Test 1.3: Return multiple dependents by specifying the dependents versionPK and its parents dependency
    print("*****Case 1.3*****")
    print("---------------------------------------------------------------------------")
    print("Returning datasets specified by multiple versionPks and a single dependency")
    print("Here we are returning 2 datasets ")
    print("---------------------------------------------------------------------------")
    try:
        for dataset in client.search(target=ds003Dependency, show="dependents",query='dependents in ({},{})'.format(ds001VersionPk, ds002VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Path: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 1.3"

    # (Case 2) .predecessors
    # Test 2.1: Print all datasets found within a path alongside their predecessor dependency metadata.
    print("*****Case 2.1*****")
    print("------------------------------------------------------------")
    print("Returns all datasets from the specified Target")
    print("Shows which ones contain dependents of type predecessor")
    print("------------------------------------------------------------")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessor", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 2.1"

    # Test 2.2: Return a single dependent by specifying the dependents versionPK and its parents dependency
    print("\n*****Case 2.2*****")
    print("-------------------------------------------------------------")
    print("Returning datasets specified by a versionPks and a dependency")
    print("-------------------------------------------------------------")
    try:
        for dataset in client.search(target=ds003Dependency,show="dependents",query='dependents in ({})'.format(ds001VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Path: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 2.2"

    # Test 2.3: Return multiple dependents by specifying the dependents versionPK and its parents dependency
    print("*****Case 2.3*****")
    print("---------------------------------------------------------------------------")
    print("Returning datasets specified by multiple versionPks and a single dependency")
    print("Here we are returning 2 datasets ")
    print("---------------------------------------------------------------------------")
    try:
        versionPkLoopValue = ds001VersionPk
        for dataset in client.search(target=ds003Dependency, show="dependents",query='dependents in ({},{})'.format(ds001VersionPk, ds002VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Path: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 2.3"

    # (Case 3) .successor
    # Test 3.1: Print all datasets found within a path alongside their predecessor dependency metadata.
    print("*****Case 3.1*****")
    print("------------------------------------------------------------")
    print("Returns all datasets from the specified Target")
    print("Shows which ones contain dependents of type successor")
    print("------------------------------------------------------------")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 3.1"

    # Test 3.2: Return a single dependent by specifying the dependents versionPK and its parents dependency
    print("\n*****Case 3.2*****")
    print("-------------------------------------------------------------")
    print("Returning datasets specified by a versionPks and a dependency")
    print("-------------------------------------------------------------")
    try:
        for dataset in client.search(target=ds006Dependency,show="dependents", query='dependents in ({})'.format(ds004VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Container: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 3.2"

    # Test 3.3: Return multiple dependents by specifying the dependents versionPK and its parents dependency
    print("*****Case 3.3*****")
    print("---------------------------------------------------------------------------")
    print("Returning datasets specified by multiple versionPks and a single dependency")
    print("Here we are returning 2 datasets ")
    print("---------------------------------------------------------------------------")
    try:
        versionPkLoopValue = ds001VersionPk
        for dataset in client.search(target=ds006Dependency, show="dependents",query='dependents in ({},{})'.format(ds004VersionPk, ds005VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Path: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 3.3"

    # Case 4: Returning a dataset located in a DIFFERENT folders but linked through a dependency
    print("*****Case 4*****")
    print("---------------------------------------------------------------------------------------")
    print("Returning datasets specified by a versionPk and dependency")
    print("The dataset being returned is located in a different folder than that of the dependency")
    print("---------------------------------------------------------------------------------------")
    try:
        for dataset in client.search(target=ds002Dependency, show="dependents",query='dependents in ({},{})'.format(ds001VersionPk_dp, ds002VersionPk_dp), ignoreShowKeyError=True):
            print(f"Name: %s" %(dataset.name))
            print(f"Dependency: %s" %(dataset.path))
            print(f"VersionPK: %s" %(dataset.versionPk))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass
            finally:
                print()
    except:
        assert False, "Error. search unsuccessful. Case 4"

    # (Case 5) Custom Dependency
    # Test 5.1 Print all datasets found within a path alongside their custom dependency metadata.
    print("*****Case 5.1*****")
    print("------------------------------------------------------------")
    print("Returns all datasets from the specified Target")
    print("Shows which ones contain dependents of type analysis__XXX")
    print("------------------------------------------------------------")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.analysis__XXX", ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Case 5.1"

    # Test 5.2: Return a single CUSTOM dependent by specifying the dependents versionPK and its parents dependency
    print("\n*****Case 5.2*****")
    print("------------------------------------------------------------")
    print("Returning datasets specified by a versionPk and dependency")
    print("------------------------------------------------------------")
    try:
        for dataset in client.search(target=dsCustom2Dependency,show="dependents", query='dependents in ({})'.format(
                dsCustom001VersionPk), ignoreShowKeyError=True):
            print(f"Dataset Name: %s" %(dataset.name))
            print(f"VersionPK: %s" %(dataset.versionPk))
            print(f"Dependency Container: %s" %(dataset.path))
            try:
                print(f"Metadata: %s" %(dict(dataset.metadata)))
            except:
                pass

    except:
        assert False, "Error. search unsuccessful. Case 5.2"
