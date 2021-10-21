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

    print("****** DATASETS CREATION BEGIN ******\n")
    # ----------------------------
    # ---creation of dataset001---
    # ----------------------------
    datacat_path01 = '/testpath/testfolder'  # Directory we are working in
    filename01 = "dataset001_82f24.dat"  # Name of dataset to be created
    metadata01 = Metadata()  # Metadata
    metadata01['nIsTest'] = 1
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
    metadata02['nIsTest'] = 1
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

    # ----------------------------
    # ---creation of dataset003---
    # ----------------------------
    datacat_path03 = '/testpath/testfolder'  # Directory we are working in
    filename03 = "dataset003_0c89c.dat"  # Name of dataset to be created
    metadata03 = Metadata()  # Metadata
    full_file003 = file_path + '/' + filename03  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path03 + '/' + filename03):
        client.rmds(datacat_path03 + '/' + filename03)

    # use the client to create dataset002 - DOES NOT initialize dependency metadata
    ds003 = client.mkds(datacat_path03, filename03, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata03,
                        resource=full_file003,
                        site='SLAC')
    ds003_version_pk = ds003.versionPk
    print("created dataset: ", filename03, "(VersionPK = ", ds003_version_pk, ")")



    print("****** API WRAPPER TEST BEGIN ******\n")

    dataset001 = ds001
    dataset002 = ds002
    dataset003 = ds003

    # ============= craete_dependency() testing starts here =============

    # Case 1.1 (general dataset) dataset doesn't have dependency metadata -> create dependency metadata
    try:
        create_dependency = client.create_dependency(dep_container=dataset001, dep_type="predecessor", dep_datasets=dataset002)
        if(create_dependency):

            created = client.path(path=dataset001.path + ";v={}".format(dataset001.versionId))
            print("dependency creation successful", created.versionMetadata)
    except:
        assert False, "dependency creation unsuccessful"

    # Case 1.2 (dataset without versionPk) dataset doesn't have dependency metadata -> create dependency metadata
    try:
        ds003_no_versionPk = client.path(path=dataset003.path)
        # create_dependency = client.create_dependency(dep_container=ds003_no_versionPk, dep_type="successor",dep_datasets=dataset001)

    except:
        assert False, "something went wrong"

    # Case 1.3 (dataset without metadata field) dataset doesn't have dependency metadata -> create dependency metadata


    # Case 2. dataset has dependency metadata -> update dependency metadata

    try:
        create_dependency = client.create_dependency()
    except:
        print("This is place holder")

    # Case 3.x Group container tests

    # client.get_dependency(group, dependentType)

    # client.delete_dependency(dependency)

    # client.get_dependents(dependency, dependentType, max_dependents=100)

    # client.add_dependents(dependency, datasets, groups)

    # client.remove_dependents(dependency, datasets, groups)





