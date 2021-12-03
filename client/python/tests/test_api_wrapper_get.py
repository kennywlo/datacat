import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


def main():

    created_datasets = create_datasets()
    created_groups = create_groups()

    create_dataset_dependencies(created_datasets)

    # Adding group as dependent to a dataset
    dataset001 = created_datasets[0]
    try:
        # initializing dependency metadata
        metadata = Metadata()
        metadata['nIsTest'] = 1

        dep_metadata = {"dependentGroups": str(created_groups.pk),
                        "dependentType": "predecessor"}
        metadata.update(dep_metadata)
        dataset001.versionMetadata.update(metadata)

        response = client.patchds(path=dataset001.path, dataset=dataset001)

    except:
        assert False, "dependent addition unsuccessful"


    # ======== get_dependents() & get_next_dependents() testing starts here ==================
    # ========================================================================================

    print("******* get_dependents() & get_next_dependents() TESTING BEGINS *******\n")
    parent_container = client.path(path=dataset001.path + ";v=current")

    dependents = []
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=4, chunk_size=3))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass

    print("")
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=4, chunk_size=3))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass

    print("")
    dependents = (client.get_next_dependents(parent_container))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass

    print("")
    dependents = (client.get_next_dependents(parent_container))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass

    print("")
    dependents = (client.get_next_dependents(parent_container))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass

    print("")
    dependents = (client.get_next_dependents(parent_container))
    try:
        for item in dependents:
            print(item.name)
    except:
        pass


def create_datasets():

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
    metadata03['nIsTest'] = 1
    full_file003 = file_path + '/' + filename03  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path03 + '/' + filename03):
        client.rmds(datacat_path03 + '/' + filename03)

    # use the client to create dataset003 - DOES NOT initialize dependency metadata
    ds003 = client.mkds(datacat_path03, filename03, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata03,
                        resource=full_file003,
                        site='SLAC')
    ds003_version_pk = ds003.versionPk
    print("created dataset: ", filename03, "(VersionPK = ", ds003_version_pk, ")")

    # ----------------------------
    # ---creation of dataset004---
    # ----------------------------
    datacat_path04 = '/testpath/testfolder'  # Directory we are working in
    filename04 = "dataset004_d8080.dat"  # Name of dataset to be created
    metadata04 = Metadata()  # Metadata
    metadata04['nIsTest'] = 1
    full_file004 = file_path + '/' + filename04  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path04 + '/' + filename04):
        client.rmds(datacat_path04 + '/' + filename04)

    # use the client to create dataset004 - DOES NOT initialize dependency metadata
    ds004 = client.mkds(datacat_path04, filename04, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata04,
                        resource=full_file004,
                        site='SLAC')
    ds004_version_pk = ds004.versionPk
    print("created dataset: ", filename04, "(VersionPK = ", ds004_version_pk, ")")


    # ----------------------------
    # ---creation of dataset005---
    # ----------------------------
    datacat_path05 = '/testpath/testfolder'  # Directory we are working in
    filename05 = "dataset005_d8080.dat"  # Name of dataset to be created
    metadata05 = Metadata()  # Metadata
    full_file005 = file_path + '/' + filename05  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path05 + '/' + filename05):
        client.rmds(datacat_path05 + '/' + filename05)

    # use the client to create dataset005 - DOES NOT initialize dependency metadata
    ds005 = client.mkds(datacat_path05, filename05, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata05,
                        resource=full_file005,
                        site='SLAC')
    ds005_version_pk = ds005.versionPk
    print("created dataset: ", filename05, "(VersionPK = ", ds005_version_pk, ")")

    return [ds001, ds002, ds003, ds004, ds005]


def create_groups():
    container_path1 = "/testpath/depGroup1"

    try:
        if client.exists(container_path1):
            client.rmdir(container_path1, type="group")

        client.mkgroup(container_path1)
        dep_group1 = client.path(path='/testpath/depGroup1;v=current')
        return dep_group1

    except:
        assert False, "Group creation failed"


def create_dataset_dependencies(created_datasets):

    dataset001 = created_datasets[0]  # used as container for add_dependents() Case 1.1, Case 2, and remove_dependents()
    dataset002 = created_datasets[1]
    dataset003 = created_datasets[2]
    dataset004 = created_datasets[3]  # without metadata field, used as container for add_dependents() Case 1.2
    dataset005 = created_datasets[4]

    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        dependents = [dataset002, dataset003]
        add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                               dep_datasets=dependents)

        if(add_dependents):
            added_after = client.path(path=dataset001.path + ";v=current")
            add_dpks = []
            for dependent in dependents:
                add_dpks.append(dependent.versionPk)

            print("Dependents added:", add_dpks, "\n@ " + dataset001.path + ";v=current\n")
    except:
        assert False, "dependent addition unsuccessful"

    try:
        added_before = client.path(path=dataset003.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset003.path + ";v=current")
        dependents = [dataset004]
        add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                               dep_datasets=dependents)

        if(add_dependents):
            added_after = client.path(path=dataset003.path + ";v=current")
            add_dpks = []
            for dependent in dependents:
                add_dpks.append(dependent.versionPk)

            print("Dependents added:", add_dpks, "\n@ " + dataset003.path + ";v=current\n")
            print("\n")
    except:
        assert False, "dependent addition unsuccessful"



if __name__ == "__main__":

    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")

    main()

