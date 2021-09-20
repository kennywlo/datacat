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
    created_datasets = create_datasets()
    dataset001 = created_datasets[0]

    # *****************************************************
    # *****************************************************
    # ********** DEPENDENCY CREATION BEGINS HERE **********
    # *****************************************************
    # *****************************************************

    datacat_path = '/testpath/testfolder'                # Search containers under Raw
    filename = "dataset003_0c89c.dat"                    # Name of dataset to be created
    metadata = Metadata()                                # Metadata <-- Dependencies will always go in metadata
    full_file003 = file_path + '/' + filename            # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)

    # Add a "dependency" to dataset003
    # (Dependencies will always be a part of the metadata)
    dependency_metadata = {"dependencyName": "test_data",                   # ?
                           "dependents": str(dataset001.versionPk),         # VersionPKs of the dependent datasets
                           "dependentType": "predecessor"}                  # [predecessor], [successor], [custom]
    metadata.update(dependency_metadata)

    # Use the client to create dataset003 along with is newly defined dependency (in metadata)
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file003,
                        site='SLAC')
    print("created dataset: ", filename, "(VersionPK = ", ds003.versionPk,")")

    # ********************************************
    # ********************************************
    # ********** CLIENT SEARCH EXAMPLES **********
    # ********************************************
    # ********************************************

    # Identify datasets containing dependency of type "predecessor" at a given target
    print("\nSearch Example 1:")
    print("-----------------------------------------------------------------------")
    print("Identifying datasets containing dependency of type predecessor")
    print("-----------------------------------------------------------------------")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.predecessor",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" %(dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" %(dataset.name))
    except:
        assert False, "Error. search unsuccessful. Search Example 1"

    # --- Now the we know what datasets have what dependency metadata, we can
    #     retrieve specific dependents, (datasets), by specifying the "dependency"
    #     as well as the "versionPk" of the dataset we wish to retrieve. ---
    print("\nSearch Example 2:")
    print("----------------------------------------------------------------------")
    print("Returning dependent dataset specified by a versionPk and a dependency")
    print("----------------------------------------------------------------------")
    try:
        # retrieve the dependency of the dataset that created the dependency
        ds003_dependency = ds003.versionMetadata["dependencyName"];

        for dataset in client.search(target=ds003_dependency,show="dependents", query='dependents in ({})'.format(
                dataset001.versionPk), ignoreShowKeyError=True):
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


def create_datasets():
    # ----------------------------
    # ---creation of dataset001---
    # ----------------------------
    datacat_path01 = '/testpath/testfolder'                # Search containers under Raw
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
    datacat_path02 = '/testpath/testfolder'                # Search containers under Raw
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

