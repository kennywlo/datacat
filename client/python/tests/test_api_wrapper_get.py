import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


def main():

    # Testing setup ==========================================================================
    # ========================================================================================


    # Create datasets needed for testing
    created_datasets = generate_datasets(10)
    generate_dependencies(created_datasets)

    # Create dataset dependencies
    #create_dataset_dependencies(created_datasets)

    # get_dependents() & get_next_dependents() testing starts here ===========================
    # ========================================================================================
    # **** For (DATASET CONTAINER) ****
    parent_container = client.path(path=created_datasets[0].path, versionId="current")

    # Case D1: Retrieving with a chunk size of 1, one dependent at a time

    print("--------------------------------------------------------------------------------\n")
    print("(Case D1): Retrieving dependents using a chunk size of 1\n")

    # Start dependent retrieval using .get_dependents
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=10, chunk_size=1))
    print("Dependents =")
    try:
        for item in dependents:
            print("\t" + item.name)
    except:
        pass
    print("")

    # Continue to retrieve dependents until there are no more left to retrieve
    while dependents != []:
        dependents = (client.get_next_dependents(parent_container))
        print("Dependents =")
        try:
            for item in dependents:
                print("\t" + item.name)
        except:
            pass
        print("")
    print("--------------------------------------------------------------------------------\n")

    # Case D2: Retrieving with a chunk size of n, In this case three dependents at a time
    print("(Case D2): Retrieving dependents using a chunk size of 3\n")
    # Start dependent retrieval using .get_dependents
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=5, chunk_size=3))
    print("Dependents =")
    try:
        for item in dependents:
            print("\t" + item.name)
    except:
        pass
    print("")

    # Continue to retrieve dependents until there are no more left to retrieve
    while dependents != []:
        dependents = (client.get_next_dependents(parent_container))
        print("Dependents =")
        try:
            for item in dependents:
                print("\t" + item.name)
        except:
            pass
        print("")
    print("--------------------------------------------------------------------------------\n")

    # Case D3: Retrieving dependents while only going 1 level deep in depth
    parent_container = client.path(path=created_datasets[0].path, versionId="current")
    print("(Case D3): Retrieving dependents while only going 1 level deep in depth\n")
    # Start dependent retrieval using .get_dependents
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=1, chunk_size=1))
    print("Dependents =")
    try:
        for item in dependents:
            print("\t" + item.name)
    except:
        pass
    print("")

    # Continue to retrieve dependents until there are no more left to retrieve
    while dependents != []:
        dependents = (client.get_next_dependents(parent_container))
        print("Dependents =")
        try:
            for item in dependents:
                print("\t" + item.name)
        except:
            pass
        print("")

    print("--------------------------------------------------------------------------------\n")


    # Case D4: Retrieving from a dependency that is currently not in cache
    parent_container = client.path(path=created_datasets[0].path, versionId="current")
    print("(Case D4): Retrieving dependents while only going 'n' levels deep in depth, in this case 3\n")
    # Start dependent retrieval using .get_dependents
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=3, chunk_size=1))
    print("Dependents =")
    try:
        for item in dependents:
            print("\t" + item.name)
    except:
        pass
    print("")

    # Continue to retrieve dependents until there are no more left to retrieve
    while dependents != []:
        dependents = (client.get_next_dependents(parent_container))
        print("Dependents =")
        try:
            for item in dependents:
                print("\t" + item.name)
        except:
            pass
        print("")

    print("--------------------------------------------------------------------------------\n")

    last_dataset = len(created_datasets) - 1
    # Case D5: Retrieving from a dependency that is currently not in cache
    parent_container = client.path(path=created_datasets[last_dataset].path, versionId="current")
    print("(Case D5): Retrieving from a dependency that is currently not in cache\n")
    # Start dependent retrieval using .get_dependents
    dependents = (client.get_dependents(parent_container, "predecessor", max_depth=5, chunk_size=3))
    print("Dependents =")
    try:
        for item in dependents:
            print("\t" + item.name)
    except:
        pass
    print("")

    print("--------------------------------------------------------------------------------\n")

    # Case D6: 50 Levels each with 2 datasets each.
    # If the last level retrieved matches with the last level saved during the creation of dependencies then we know
    # that the retrievals are working as intended

    datasets_for_stress_test = generate_datasets(101)
    last_level_returned = generate_dependencies_stress_test(datasets_for_stress_test)
    last_level_calculated = []
    last_level_calculated_datasets = []

    print("(Case D6): Generating 50 levels each with two dependent datasets, testing for proper retrieval of last level ")

    parent_container = client.path(path=datasets_for_stress_test[0].path, versionId="current")
    dependents = client.get_dependents(parent_container, "predecessor", max_depth=150, chunk_size=2)

    while dependents != []:
        dependents = (client.get_next_dependents(parent_container))

        if dependents != []:
            last_level_calculated.clear()
            last_level_calculated_datasets.clear()
        try:
            for item in dependents:
                last_level_calculated.append(item.versionPk)
                last_level_calculated_datasets.append(item.name)
        except:
            pass

    print("\nLast Level of dependents retrieved")
    for x in last_level_calculated_datasets:
        print("\t" + x)

    print("\nComparing expected value to returned value... If no assert failure occurs then the values are a match. ")
    assert (last_level_calculated == last_level_returned), "Last level not matching... Dependent retrieval incorrect"





    # Case D2:
    # This test case should make sure that if given a DATASET container we can retrieve ONLY GROUP dependents from it
    # This test case will also make sure that we can continue to retrieve using .get_next_dependents

    # Case D3:
    # /* This test case should make sure that if given a DATASET container we can retrieve A COMBINATION of GROUP and
    # DATASET dependents from it */

    # **** (GROUP CONTAINER) ****
    # Case G1:
    # This test case should make sure that if given a GROUP container we can retrieve ONLY DATASET dependents from it
    # This test case will also make sure that we can continue to retrieve using .get_next_dependents

    # Case G2:
    # This test case should make sure that if given a GROUP container we can retrieve ONLY GROUP dependents from it
    # This test case will also make sure that we can continue to retrieve using .get_next_dependents

    # Case G3:
    # /* This test case should make sure that if given a GROUP container we can retrieve A COMBINATION of GROUP and
    # DATASET dependents from it */

    # *************************************************************************************************************
    # *************************************************************************************************************
    # In order to verify the accuracy of the retrievals being provided by .get_dependents and get_next_dependents I
    # propose the following test.
    #
    # Because the structure of dependency follows that of a tree, we can assume that if we retrieve the wrong
    # dependents at a lower level then all of the retrieved dependents at a higher level will be incorrect. This
    # allows us to test the accuracy of retrieval by comparing the last level retrieved to a stored instance of the
    # last level saved upon addition of the dependencies. If these two match then we know that the algorithm devised
    # is indeed following the proper paths and returning all the dependents in the correct order.
    # *************************************************************************************************************
    # *************************************************************************************************************


def generate_datasets(number_to_generate):

    dataset_return = []

    for x in range(number_to_generate):

        datacat_path_generic = '/testpath/testfolder'  # Directory we are working in
        filename_generic = "dataset_" + str(x) + ".dat"  # Name of dataset to be created
        metadata_generic = Metadata()  # Metadata
        metadata_generic['nIsTest'] = 1
        full_file_generic = file_path + '/' + filename_generic  # ../../../test/data/ + filename

        # Check to make sure the dataset doesnt already exist at the provided path
        if client.exists(datacat_path_generic + '/' + filename_generic):
            client.rmds(datacat_path_generic + '/' + filename_generic)

        # Use the client to create a new dataset using the params mentioned above
        ds_generic = client.mkds(datacat_path_generic, filename_generic, 'JUNIT_TEST', 'junit.test',
                            versionMetadata=metadata_generic,
                            resource=full_file_generic,
                            site='SLAC')
        ds_generic_version_pk = ds_generic.versionPk
        print("created dataset: ", filename_generic, "(VersionPK = ", ds_generic_version_pk, ")")

        dataset_return.append(ds_generic)

    return dataset_return


def generate_dependencies(list_of_datasets):

    level_counter = 1

    # Level 1
    print(len(list_of_datasets))
    for dataset in range(len(list_of_datasets)-1):

        try:
            added_before = client.path(path=list_of_datasets[dataset].path, versionId="current")
            dataset_to_Patch = client.path(path=list_of_datasets[dataset].path, versionId="current")
            dependents = [list_of_datasets[dataset+1]]
            add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                   dep_datasets=dependents)

            if(add_dependents):
                added_after = client.path(path=list_of_datasets[dataset].path, versionId="current")
                add_dpks = []
                for dependent in dependents:
                    add_dpks.append(dependent.versionPk)
                print("\nLevel: " + str(level_counter))
                print("Dependents added:", add_dpks, "\n@ " + list_of_datasets[dataset].path + ";v=current\n")
                level_counter = level_counter + 1
        except:
            assert False, "dependent addition unsuccessful"


# This will add two dependents per level instead of the original 1 dataset per level.
# We can use this to test the accuracy of the get methods
def generate_dependencies_stress_test(list_of_datasets):

    last_level_of_dependents = []
    level_counter = 1

    # Level 1
    print(len(list_of_datasets))
    for datasetIndex in range(0, len(list_of_datasets)-1, 2):

        try:
            added_before = client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
            dataset_to_Patch = client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
            dependents = [list_of_datasets[datasetIndex+1], list_of_datasets[datasetIndex+2]]
            last_level_of_dependents.clear()
            for x in dependents:
                last_level_of_dependents.append(x.versionPk)
            add_dependents = client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                   dep_datasets=dependents)

            if(add_dependents):
                added_after = client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
                add_dpks = []
                for dependent in dependents:
                    add_dpks.append(dependent.versionPk)

                print("\nLevel: " + str(level_counter))
                print("Dependents added:", add_dpks, "\n@ " + list_of_datasets[datasetIndex].path + ";v=current\n")
                level_counter = level_counter + 1

        except:
            assert False, "dependent addition unsuccessful"

    return last_level_of_dependents


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





if __name__ == "__main__":

    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")

    main()