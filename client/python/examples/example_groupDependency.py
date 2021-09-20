import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

# datacat
config_file ='./config_srs.ini'
config = config_from_file(config_file)
client = client_from_config(config)

# file/datacatalog path
file_path = os.path.abspath("../../../test/data/")


def main():

    # Will generate 2 datasets, in this case we are only using 1 of the generated 2
    created_datasets = create_datasets()
    dataset001 = created_datasets[0]

    # *****************************************************
    # *****************************************************
    # ******* GROUP DEPENDENCY CREATION BEGINS HERE *******
    # *****************************************************
    # *****************************************************

    # adding the datasets with their dependentType to a groups metadata (predecessor)
    metadata = Metadata()
    dep_metadataPredecessor = {
        "dependents": str(dataset001.versionPk),
        "dependentType": "predecessor"
    }
    metadata.update(dep_metadataPredecessor)

    # Declare the path you wish to have your new group at
    container_path_predecessor = "/testpath/dependencyGroup"

    # Check to see if group already exists at the path, if it does... delete old group
    try:
        if client.exists(container_path_predecessor):
            client.rmdir(container_path_predecessor, type="group")
    except:
        print("exception caught here")

    # Use the client to create the new group alongside its new dependency metadata
    dep_group_predecessor = client.mkgroup(container_path_predecessor, metadata=metadata)
    print("\nCreated New Group as:\n{} \nMetadata: {}".format(dep_group_predecessor, dict(metadata)))

    # ********************************************
    # ********************************************
    # ********** CLIENT SEARCH EXAMPLES **********
    # ********************************************
    # ********************************************

    # Retrieves all predecessor datasets linked to the group
    print("\nSearch Example 1:")
    print("--------------------------------------------------------------------------------------")
    print("Return the group found at the provided path alongside its dependents of specified type")
    print("--------------------------------------------------------------------------------------")
    try:
        print(client.path(path='/testpath/dependencyGroup;metadata=dependents.predecessor'))
    except:
        assert False, "Error. search unsuccessful. Search Example 1"

    # Return specified dependents (datasets)
    print("\nSearch Example 2:")
    print("-----------------------------------------------------------------------")
    print("Return the dependent specified by the target and query parameter")
    print("-----------------------------------------------------------------------")

    # -- Through this client call we are able to specify the path of a "group" which contains a dependency.
    #    If we know the exact dependent we want back we can retrieve it by providing its versionPk to the
    #    query -- (query='dependents in ({})'.format(dataset001.versionPk))

    try:
        for dataset in client.search(target='/testpath/dependencyGroup', show="dependents.predecessor", query='dependents in ({})'.format(dataset001.versionPk), ignoreShowKeyError=True):
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
        assert False, "Error. search unsuccessful. Search Example 2"

    # End of example

def create_datasets():
    # ----------------------------
    # ---creation of dataset001---
    # ----------------------------
    datacat_path01 = '/testpath/testfolder'                # Directory we are working in
    filename01 = "dataset001_82f24.dat"                    # Name of dataset to be created
    metadata01 = Metadata()                                # Metadata
    full_file001 = file_path + '/' + filename01            # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path01 + '/' + filename01):
        client.rmds(datacat_path01 + '/' + filename01)

    # Use the client to create a new dataset using the params mentioned above
    ds001 = client.mkds(datacat_path01, filename01, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata01,
                        resource=full_file001,
                        site='SLAC')
    ds001_version_pk = ds001.versionPk
    print("created dataset: ", filename01, "(VersionPK = ", ds001_version_pk,")")

    # ----------------------------
    # ---creation of dataset002---
    # ----------------------------
    datacat_path02 = '/testpath/testfolder'                # Directory we are working in
    filename02 = "dataset002_92e56.dat"                    # Name of dataset to be created
    metadata02 = Metadata()                                # Metadata
    full_file002 = file_path + '/' + filename02            # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path02 + '/' + filename02):
        client.rmds(datacat_path02 + '/' + filename02)

    # use the client to create dataset002 - DOES NOT initialize dependency metadata
    ds002 = client.mkds(datacat_path02, filename02, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata02,
                        resource=full_file002,
                        site='SLAC')
    ds002_version_pk = ds002.versionPk
    print("created dataset: ", filename02, "(VersionPK = ", ds002_version_pk,")")

    return [ds001, ds002]

if __name__ == "__main__":
    main()




