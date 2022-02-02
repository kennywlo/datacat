import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata, DatasetLocation


def main():
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
    print("\ncreated dataset: ", filename, "(For Utility Testing) (VersionPK = ", ds001VersionPk,")")


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
    print("\ncreated dataset: ", filename, "(For Utility Testing) (VersionPK = ", ds002VersionPk,")")

    # creation of dataset003
    filename = "dataset003.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    full_file003 = file_path + '/' + filename
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file003,
                        site='SLAC')
    ds003VersionPk = ds003.versionPk
    print("\ncreated dataset: ", filename, "(For mkloc Testing) (VersionPK = ", ds003VersionPk,")")
    # **************************************************
    # **************************************************
    # ********** DIRECTORY CREATION STARTS HERE ********
    # **************************************************
    # **************************************************


    # Declare the path you wish to have your new group at
    container_path_predecessor = "/testpath/dependencyGroup"

    # Check to see if group already exists at the path, if it does... delete old group
    try:
        if client.exists(container_path_predecessor):
            client.rmdir(container_path_predecessor, type="group")
    except:
        print("exception caught here")

    # Use the client to create the new group alongside its new dependency metadata
    group = client.mkgroup(container_path_predecessor,metadata=metadata)
    print("\nCreated New Group as:\n{}".format(group))


    # **************************************************
    # **************************************************
    # ********** DATASET PATCHING STARTS HERE **********
    # **************************************************
    # **************************************************

    # Printing the dependency relations before patch
    print_dependency_before(client)

    # Retrieving Dataset to patch
    datasetToPatch = client.path(path="/testpath/testfolder/dataset002_92e56.dat;v=current")

    # Creating the dependency information
    dependency_metadata = {"dependents": str(ds001.versionPk),         # VersionPKs of the dependent datasets
                      "dependentType": "predecessor"}                  # [predecessor], [successor], [custom]
    # Updating the metadata
    datasetToPatch.versionMetadata.update(dependency_metadata)

    # Patching the updated metadata
    ds002_return = client.patchds(path="/testpath/testfolder/dataset002_92e56.dat", dataset=datasetToPatch)

    # Printing the dependency relations after patch
    print_dependency_after(client)

    # **************************************************
    # **************************************************
    # ********** DIRECTORY PATCHING STARTS HERE ********
    # **************************************************
    # **************************************************
    metadata = Metadata()

    dep_metadataPredecessor = {
    "dependents": str(ds001VersionPk),
    "dependentType": "predecessor"
    }

    metadata.update(dep_metadataPredecessor)
    group.metadata.update(dep_metadataPredecessor)

    returnedGroup = client.patchdir(path=container_path_predecessor, container=group, type="group")
    print(returnedGroup)

    # *********** Testing mkloc ***********
    print("client.mkloc test begins")
    client.mkloc(path="/testpath/testfolder/dataset003.dat", site="OSN", resource=full_file003)
    ds003_return = client.path(path="/testpath/testfolder/dataset003.dat", versionId="current")
    print("client.mkloc test result:")
    print("dataset003 location:",ds003_return.locations)


    # *********** Testing creating dataset with 2 locations ***********
    filename = "dataset004.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    full_file004 = file_path + '/' + filename

    location1 = DatasetLocation(resource=full_file004, site='SLAC')
    location2 = DatasetLocation(resource=full_file004, site='OSN')
    ds004 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        locations=[location1, location2])
    ds004_return = client.path(path="/testpath/testfolder/dataset004.dat", versionId="current")
    print("dataset004 location:", ds004_return.locations)


def print_dependency_before(client):
    # Printing dependency relations prior to the patch
    print("\nBefore Patch:")
    print("-----------------------------------------------------------------------")
    print("Datasets containing dependency of type predecessor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"

    print("\nDatasets containing dependency of type successor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"
    print("-----------------------------------------------------------------------")


def print_dependency_after(client):
    # Printing dependency relations prior to the patch
    print("\nBefore Patch:")
    print("-----------------------------------------------------------------------")
    print("Datasets containing dependency of type predecessor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"

    print("\nDatasets containing dependency of type successor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"
    print("-----------------------------------------------------------------------")


if __name__ == "__main__":
    main()
