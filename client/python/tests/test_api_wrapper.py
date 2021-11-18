import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


def main():

    created_datasets = create_datasets()
    dataset001 = created_datasets[0] # used as container for add_dependents() Case 1.1, Case 2, and remove_dependents()
    dataset002 = created_datasets[1]
    dataset003 = created_datasets[2]
    dataset004 = created_datasets[3] # without metadata field, used as container for add_dependents() Case 1.2


    print("\n")

    # ============= add_dependents() testing starts here =============
    # ================================================================
    print("******* add_dependents() TESTING BEGINS *******\n")
    # Case 1.1 (general dataset) dataset doesn't have dependency metadata -> add dependency metadata
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

            print("Dependents to add dependents:", add_dpks)
            print("Case 1.1 dependent addition successful:")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Case 1.1 dependent addition unsuccessful"

    # Case 1.2 (dataset without metadata field) dataset doesn't have dependency metadata -> add dependency metadata
    try:
        dataset_to_Patch = client.path(path=dataset004.path + ";v=current")
        print("Dataset to add:", dataset004)
        add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                               dep_datasets=dependents)

        if(add_dependents):
            added_after = client.path(path=dataset004.path + ";v=current")
            print("Case 1.2 dependenct addition successful: ")
            print("OLD METADATA OUTPUT:", None)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Case 1.2 dependency creation unsuccessful"

    # Case 2 dataset has dependency metadata -> update dependency metadata
    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        update_dependents = [dataset003,dataset004]
        add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                               dep_datasets=update_dependents)

        if(add_dependents):
            update_dpks = []
            for dependent in update_dependents:
                update_dpks.append(dependent.versionPk)
            print("Case 2 dependenct update successful:")
            print("Dependents to update:", update_dpks)
            added_after = client.path(path=dataset001.path + ";v=current")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Case 2 dependent addition unsuccessful"

    # Case 3.x Group as container dependency testing



    # ======== get_dependents() & get_next_dependents() testing starts here ==================
    # ========================================================================================

    # print("******* get_dependents() & get_next_dependents() TESTING BEGINS *******\n")
    # parent_container = client.path(path=dataset001.path + ";v=current")
    #
    #
    # dependents = []
    # dependents = (client.get_dependents(parent_container, "predecessor"))
    # print(dependents)


    # ============= remove_dependents() testing starts here =============
    # ===================================================================
    print("******* remove_dependents() TESTING BEGINS *******\n")

    # Case 1.1 remove all dependency version metadata
    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        remove_dependents = [dataset003, dataset004, dataset002]
        add_dependents = client.remove_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                  dep_datasets=remove_dependents)

        if(add_dependents):
            remove_dpks = []
            for dependent in remove_dependents:
                remove_dpks.append(dependent.versionPk)
            added_after = client.path(path=dataset001.path + ";v=current")
            print("Dependents to be removed:", remove_dpks)
            if "predecessor.dataset" in added_after.versionMetadata:
                assert False, "Dependents are not completely removed. " \
                              "{} remains in versionMetadata." \
                    .format(added_after.versionMetadata["predecessor.dataset"])

            print("Dependent removal successful:")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Dependents removal unsuccessful"





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
    full_file004 = file_path + '/' + filename04  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path04 + '/' + filename04):
        client.rmds(datacat_path04 + '/' + filename04)

    # use the client to create dataset004 - DOES NOT initialize dependency metadata
    ds004 = client.mkds(datacat_path03, filename04, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata04,
                        resource=full_file004,
                        site='SLAC')
    ds004_version_pk = ds004.versionPk
    print("created dataset: ", filename04, "(VersionPK = ", ds004_version_pk, ")")

    return [ds001, ds002, ds003, ds004]


if __name__ == "__main__":

    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")

    main()








