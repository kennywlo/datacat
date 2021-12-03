import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


def main():

    created_datasets = create_datasets()
    dataset001 = created_datasets[0] # used as container for add_dependents() Case 1.1, Case 2, and remove_dependents()
    dataset002 = created_datasets[1]
    dataset003 = created_datasets[2]
    dataset004 = created_datasets[3] # without metadata field, used as container for add_dependents() Case 1.2
    dataset005 = created_datasets[4]

    created_groups = create_groups()
    group1 = created_groups[0]
    group2 = created_groups[1]
    group3 = created_groups[2]
    group4 = created_groups[3]
    group5 = created_groups[4]


    print("\n")

    # ============= add_dependents() testing starts here =============
    # ================================================================
    print("******* add_dependents() TESTING BEGINS *******\n")

    """
    
    ****************************************************
    *** The following are dataset as container tests ***
    ****************************************************
    
    """


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

    # Case 2.1 dataset has dependency metadata of same dependent type -> add new dependents to same dependent type
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
            print("Case 2.1 dependenct update successful:")
            print("Dependents to update:", update_dpks)
            added_after = client.path(path=dataset001.path + ";v=current")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Case 2 dependent addition unsuccessful"

    # Case 2.2 dataset has dependency metadata of different dependent type -> add dependents to new dependent type
    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        update_dependents = [dataset005]
        add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="successor",
                                               dep_datasets=update_dependents)

        if(add_dependents):
            update_dpks = []
            for dependent in update_dependents:
                update_dpks.append(dependent.versionPk)
            print("Case 2.2 dependent update successful:")
            print("Dependents to update:", update_dpks)
            added_after = client.path(path=dataset001.path + ";v=current")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Case 2 dependent addition unsuccessful"

    """
    
    **************************************************
    *** The following are group as container tests ***
    **************************************************
    
    """



    # Case 3.1 Attach dataset to group container
    try:
        added_before = client.path(path='/testpath/depGroup1;v=current')
        group_to_patch = client.path(path='/testpath/depGroup1;v=current')
        update_dependents = [dataset001, dataset002]
        add_dependents = client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                               dep_datasets=update_dependents)


        group_md = None
        if hasattr(added_before, "metadata"):
            group_md = added_after.metadata

        if add_dependents:
            update_dpks = []
            for dependent in update_dependents:
                update_dpks.append(dependent.versionPk)

            expected = [('dependencyName','{}'.format(group_to_patch.path)), ('predecessor.dataset','{},{}'.format(update_dpks[0],update_dpks[1]))]
            added_after = client.path(path='/testpath/depGroup1;metadata=dependents')

            if len(expected) != len(added_after) or set(expected) != set(added_after):
                assert False, "Case 3.1 group dependent addition result is not as expected: {}.\nExpected: {}".format(added_after, expected)

            print("Case 3.1 group dependent addition successful:")
            print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
            print("CONTAINER GROUP NAME:", added_before.name)
            print("CONTAINER GROUP OLD METADATA:", group_md)
            print("UPDATED METADATA OUTPUT:", added_after)
            print("\n")

    except:
        assert False, "Case 3.1 group dependent addition unsuccessful"

    # Case 3.2 Attach group to group container
    try:
        added_before = client.path(path='/testpath/depGroup2;v=current')
        group_to_patch = client.path(path='/testpath/depGroup2;v=current')
        update_dependents = [group3]
        add_dependents = client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                               dep_groups=update_dependents)

        group_md = None
        if hasattr(added_before, "metadata"):
            group_md = added_after.metadata


        if add_dependents:
            update_gpks = []
            for dependent in update_dependents:
                update_gpks.append(dependent.pk)

            added_after = client.path(path='/testpath/depGroup2;metadata=dependents')

            expected = [('predecessor.group','{}'.format(update_gpks[0])), ('dependencyName','{}'.format(group_to_patch.path))]

            if len(expected) != len(added_after) or set(expected) != set(added_after):
                assert False, "Case 3.2 group dependent addition result is not as expected: {}.\nExpected: {}".format(added_after, expected)

            print("Case 3.2 group dependent addition successful:")
            print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
            print("CONTAINER GROUP NAME:", added_before.name)
            print("CONTAINER GROUP OLD METADATA:", group_md)
            print("UPDATED METADATA OUTPUT:", added_after)
            print("\n")

    except:
        assert False, "Case 3.2 group dependent addition unsuccessful"




    # Case 3.3 Attach dataset AND group to a group container
    try:
        added_before = client.path(path='/testpath/depGroup3;v=current')
        group_to_patch = client.path(path='/testpath/depGroup3;v=current')
        update_dependent_groups = [group4]
        update_dependent_datasets = [dataset001, dataset002]

        add_dependents = client.add_dependents(dep_container=group_to_patch,
                                               dep_type="predecessor",
                                               dep_groups=update_dependent_groups,
                                               dep_datasets=update_dependent_datasets)

        group_md = None
        if hasattr(added_before, "metadata"):
            group_md = added_after.metadata


        if add_dependents:
            update_gpks = []
            update_dpks = []
            for dependent in update_dependent_datasets:
                update_dpks.append(dependent.versionPk)

            for dependent in update_dependent_groups:
                update_gpks.append(dependent.pk)

            added_after = client.path(path='/testpath/depGroup3;metadata=dependents')

            expected = [('predecessor.group', '{}'.format(update_gpks[0])), ('predecessor.dataset', '{},{}'.format(update_dpks[0],update_dpks[1])),('dependencyName','{}'.format(group_to_patch.path))]

            if len(expected) != len(added_after) or set(expected) != set(added_after):
                assert False, "Case 3.2 group dependent addition result is not as expected: {}.\nExpected: {}".format(added_after, expected)

            print("Case 3.3 group dependent addition successful:")
            print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
            print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
            print("CONTAINER GROUP NAME:", added_before.name)
            print("CONTAINER GROUP OLD METADATA:", group_md)
            print("UPDATED METADATA OUTPUT:", added_after)
            print("\n")

    except:
        assert False, "Case 3.3 group dependent addition unsuccessful"



    # ======== get_dependents() & get_next_dependents() testing starts here ==================
    # ========================================================================================


    print("\n******* get_dependents() & get_next_dependents() TESTING BEGINS *******\n")
    parent_container = client.path(path=dataset001.path + ";v=current")


    dependents = []
    dependents = (client.get_dependents(parent_container, "predecessor",max_depth=2, chunk_size=2))
    for item in dependents:
        print(item.name)


    # ============= remove_dependents() testing starts here =============
    # ===================================================================
    print("\n******* remove_dependents() TESTING BEGINS *******\n")

    # Case 1.1 remove user requested dependency version metadata
    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        remove_dependents = [dataset002]
        add_dependents = client.remove_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                  dep_datasets=remove_dependents)

        if(add_dependents):
            remove_dpks = []
            for dependent in remove_dependents:
                remove_dpks.append(dependent.versionPk)
            added_after = client.path(path=dataset001.path + ";v=current")
            print("Dependents to be removed:", remove_dpks)

            print("Case 1.1 dependent removal successful:")
            print("OLD METADATA OUTPUT:", added_before.versionMetadata)
            print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
            print("\n")
    except:
        assert False, "Dependents removal unsuccessful"

    # Case 1.2 remove all dependency version metadata
    try:
        added_before = client.path(path=dataset001.path + ";v=current")
        dataset_to_Patch = client.path(path=dataset001.path + ";v=current")
        remove_dependents = [dataset003, dataset004]
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

            print("Case 1.2 dependent removal successful:")
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
    filename05 = "dataset005.dat"  # Name of dataset to be created
    metadata05 = Metadata()  # Metadata
    full_file005 = file_path + '/' + filename05  # ../../../test/data/ + filename

    # Check to make sure the dataset doesnt already exist at the provided path
    if client.exists(datacat_path05 + '/' + filename05):
        client.rmds(datacat_path05 + '/' + filename05)

    # use the client to create dataset004 - DOES NOT initialize dependency metadata
    ds005 = client.mkds(datacat_path05, filename05, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata05,
                        resource=full_file005,
                        site='SLAC')
    ds005_version_pk = ds005.versionPk
    print("created dataset: ", filename05, "(VersionPK = ", ds005_version_pk, ")")

    return [ds001, ds002, ds003, ds004, ds005]

def create_groups():

    print("\n****** GROUPS CREATION BEGIN ******\n")

    containerPath1 = "/testpath/depGroup1"
    containerPath2 = "/testpath/depGroup2"
    containerPath3 = "/testpath/depGroup3"
    containerPath4 = "/testpath/depGroup4"
    containerPath5 = "/testpath/depGroup5"

    try:
        if client.exists(containerPath1):
            client.rmdir(containerPath1, type="group")

        if client.exists(containerPath2):
            client.rmdir(containerPath2, type="group")

        if client.exists(containerPath3):
            client.rmdir(containerPath3, type="group")

        if client.exists(containerPath4):
            client.rmdir(containerPath4, type="group")

        if client.exists(containerPath5):
            client.rmdir(containerPath5, type="group")


        client.mkgroup(containerPath1)
        client.path(path='/testpath/depGroup1;v=current')
        group1 = client.path(path='{};v=current'.format(containerPath1))
        print("created group:", group1.name, "(Pk {})".format(group1.pk))


        client.mkgroup(containerPath2)
        group2 = client.path(path='{};v=current'.format(containerPath2))
        print("created group:", group2.name, "(Pk {})".format(group2.pk))

        client.mkgroup(containerPath3)
        group3 = client.path(path='{};v=current'.format(containerPath3))
        print("created group:", group3.name, "(Pk {})".format(group3.pk))


        client.mkgroup(containerPath4)
        group4 = client.path(path='{};v=current'.format(containerPath4))
        print("created group:", group4.name, "(Pk {})".format(group4.pk))

        client.mkgroup(containerPath5)
        group5 = client.path(path='{};v=current'.format(containerPath5))
        print("created group:", group5.name, "(Pk {})".format(group5.pk))


        return [group1, group2, group3, group4, group5]

    except:
        assert False, "Group creation failed"




if __name__ == "__main__":

    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")

    main()




