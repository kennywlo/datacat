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
    print("****** API WRAPPER TEST BEGIN ******\n")
    create_datasets()

    # client.create_dependency(group, datasets, groups, dependentType)

    # client.get_dependency(group, dependentType)

    # client.delete_dependency(dependency)

    # client.get_dependents(dependency, dependentType, max_dependents=100)

    # client.add_dependents(dependency, datasets, groups)

    # client.remove_dependents(dependency, datasets, groups)


def create_datasets():
    # ----------------------------
    # ---creation of dataset001---
    # ----------------------------
    datacat_path01 = '/testpath/testfolder'  # Directory we are working in
    filename01 = "dataset001_82f24.dat"  # Name of dataset to be created
    metadata01 = Metadata()  # Metadata
    full_file001 = file_path + '/' + filename01  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path01 + '/' + filename01):
        client.rmds(datacat_path01 + '/' + filename01)

    # Use the client to create a new dataset using the params mentioned above
    ds001 = client.mkds(datacat_path01, filename01, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata01,
                        resource=full_file001,
                        site='SLAC')
    ds001_version_pk = ds001.versionPk
    print("created dataset: ", filename01, "(VersionPK = ", ds001_version_pk, ")")

    # ----------------------------
    # ---creation of dataset002---
    # ----------------------------
    datacat_path02 = '/testpath/testfolder'  # Directory we are working in
    filename02 = "dataset002_92e56.dat"  # Name of dataset to be created
    metadata02 = Metadata()  # Metadata
    full_file002 = file_path + '/' + filename02  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path02 + '/' + filename02):
        client.rmds(datacat_path02 + '/' + filename02)

    # use the client to create dataset002 - DOES NOT initialize dependency metadata
    ds002 = client.mkds(datacat_path02, filename02, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata02,
                        resource=full_file002,
                        site='SLAC')
    ds002_version_pk = ds002.versionPk
    print("created dataset: ", filename02, "(VersionPK = ", ds002_version_pk, ")")

    return [ds001, ds002]


if __name__ == "__main__":
    main()
