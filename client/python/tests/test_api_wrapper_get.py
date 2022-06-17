import unittest
import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


class ApiWrapperGet(unittest.TestCase):
    created_datasets = None
    client = None

    def setUp(self):
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        self.client = client_from_config(config)

        # Testing setup ==========================================================================
        # ========================================================================================

        # Create datasets needed for testing
        self.created_datasets = self.generate_datasets(10)

        # Create the need ed dependencies
        self.generate_dependencies(self.created_datasets)

        # **** For (DATASET CONTAINER) ****
        self.parent_container = self.client.path(path=self.created_datasets[0].path, versionId="current")

    def test_case_one(self):
        # ******************************************************************
        # ******************************************************************
        # Case 1: Retrieving with a chunk size of 1, one dependent at a time
        # ******************************************************************
        # ******************************************************************
        print("--------------------------------------------------------------------------------")
        print("(Case 1): Retrieving dependents using a chunk size of 1\n")

        all_dependents = []
        expected_dependents = ['dataset_1.dat', 'dataset_2.dat', 'dataset_3.dat', 'dataset_4.dat', 'dataset_5.dat']

        # Start dependent retrieval using .get_dependents
        dependents = (self.client.get_dependents(self.parent_container, "predecessor", max_depth=5, chunk_size=1))
        print("Dependents =")
        try:
            for item in dependents:
                all_dependents.append(item.name)
                print("\t" + item.name)
        except:
            pass
        print("")

        # Continue to retrieve dependents until there are no more left to retrieve
        while dependents:
            dependents = (self.client.get_next_dependents(self.parent_container))
            print("Dependents =")
            try:
                for item in dependents:
                    all_dependents.append(item.name)
                    print("\t" + item.name)
            except:
                pass
            print("")

        assert (all_dependents == expected_dependents), \
            "(Case 1 Failure : Expected dependents don't match returned dependents)"
        all_dependents.clear()

    def test_case_two(self):
        # ******************************************************************
        # ******************************************************************
        # Case 2: Retrieving with a chunk size of n, In this case three dependents at a time
        # ******************************************************************
        # ******************************************************************
        print("--------------------------------------------------------------------------------")
        print("(Case 2): Retrieving dependents using a chunk size of 3\n")

        all_dependents = []
        expected_dependents_get = ['dataset_1.dat', 'dataset_2.dat', 'dataset_3.dat']
        expected_dependents_get_next = ['dataset_4.dat', 'dataset_5.dat']

        # Start dependent retrieval using .get_dependents
        dependents = (self.client.get_dependents(self.parent_container, "predecessor", max_depth=5, chunk_size=3))
        print("Dependents =")
        try:
            for item in dependents:
                all_dependents.append(item.name)
                print("\t" + item.name)
        except:
            pass
        print("")

        assert (expected_dependents_get == all_dependents), \
            "(Case 2 Failure : Expected dependents don't match returned dependents)"
        all_dependents.clear()

        # Continue to retrieve dependents until there are no more left to retrieve
        while dependents:
            dependents = (self.client.get_next_dependents(self.parent_container))
            print("Dependents =")
            try:
                for item in dependents:
                    all_dependents.append(item.name)
                    print("\t" + item.name)
            except:
                pass
            print("")

        assert (expected_dependents_get_next == all_dependents), \
            "(Case 2 Failure : Expected dependents don't match returned dependents)"
        all_dependents.clear()

    def test_case_three(self):
        # ******************************************************************
        # ******************************************************************
        # Case 3: Retrieving dependents while only going 1 level deep in depth
        # ******************************************************************
        # ******************************************************************
        print("--------------------------------------------------------------------------------")
        parent_container = self.client.path(path=self.created_datasets[0].path, versionId="current")
        print("(Case 3): Retrieving dependents while only going 1 level deep in depth\n")

        all_dependents = []
        expected_dependents = ['dataset_1.dat']

        # Start dependent retrieval using .get_dependents
        dependents = (self.client.get_dependents(parent_container, "predecessor", max_depth=1, chunk_size=1))
        print("Dependents =")
        try:
            for item in dependents:
                all_dependents.append(item.name)
                print("\t" + item.name)
        except:
            pass
        print("")

        # Continue to retrieve dependents until there are no more left to retrieve
        while dependents:
            dependents = (self.client.get_next_dependents(parent_container))
            print("Dependents =")
            try:
                for item in dependents:
                    all_dependents.append(item.name)
                    print("\t" + item.name)
            except:
                pass
            print("")

        assert (expected_dependents == all_dependents), \
            "(Case 3 Failure : Expected dependents don't match returned dependents)"
        all_dependents.clear()

    def test_case_four(self):
        # ******************************************************************
        # ******************************************************************
        # Case 4: Retrieving dependents while only going 'n' levels deep in depth, in this case 3
        # ******************************************************************
        # ******************************************************************
        print("--------------------------------------------------------------------------------")
        parent_container = self.client.path(path=self.created_datasets[0].path, versionId="current")
        print("(Case 4): Retrieving dependents while only going 'n' levels deep in depth, in this case 3\n")

        all_dependents = []
        expected_dependents = ['dataset_1.dat', 'dataset_2.dat', 'dataset_3.dat']

        # Start dependent retrieval using .get_dependents
        dependents = (self.client.get_dependents(parent_container, "predecessor", max_depth=3, chunk_size=1))
        print("Dependents =")
        try:
            for item in dependents:
                all_dependents.append(item.name)
                print("\t" + item.name)
        except:
            pass
        print("")

        # Continue to retrieve dependents until there are no more left to retrieve
        while dependents:
            dependents = (self.client.get_next_dependents(parent_container))
            print("Dependents =")
            try:
                for item in dependents:
                    all_dependents.append(item.name)
                    print("\t" + item.name)
            except:
                pass
            print("")

        assert (expected_dependents == all_dependents), \
            "(Case 4 Failure : Expected dependents don't match returned dependents)"
        all_dependents.clear()

    def test_case_five(self):
        # ******************************************************************
        # ******************************************************************
        # Case D5: Retrieving from a dependency that is currently not in cache
        # ******************************************************************
        # ******************************************************************

        last_dataset = len(self.created_datasets) - 1

        print("--------------------------------------------------------------------------------")
        parent_container = self.client.path(path=self.created_datasets[last_dataset].path, versionId="current")
        print("(Case 5): Retrieving from a dependency that is currently not in cache\n")
        # Start dependent retrieval using .get_dependents
        dependents = (self.client.get_dependents(parent_container, "predecessor", max_depth=5, chunk_size=3))
        print("Dependents =")
        try:
            for item in dependents:
                print("\t" + item.name)
        except:
            pass
        print("")

        assert (dependents == []), \
            "(Case 5 Failure : Returned dependents should be [])"

    def test_case_six(self):
        # ******************************************************************
        # ******************************************************************
        # Case 6: 50 Levels each with 2 datasets each.
        # If the last level retrieved matches with the last level saved during the creation of dependencies then we know
        # that the retrievals are working as intended
        # ******************************************************************
        # ******************************************************************

        datasets_for_stress_test = self.generate_datasets(101)
        last_level_returned = self.generate_dependencies_stress_test(datasets_for_stress_test)
        last_level_calculated = []
        last_level_calculated_datasets = []

        print("--------------------------------------------------------------------------------\n")
        print("(Case 6): Generating 50 levels each with two dependent datasets, testing for proper retrieval of last "
              "level")

        parent_container = self.client.path(path=datasets_for_stress_test[0].path, versionId="current")
        dependents = self.client.get_dependents(parent_container, "predecessor", max_depth=150, chunk_size=2)

        while dependents:
            dependents = (self.client.get_next_dependents(parent_container))

            if dependents:
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

        print(
            "\nComparing expected value to returned value... If no assert failure occurs then the values are a match. ")
        assert (
                last_level_calculated == last_level_returned), "Last level not matching... Dependent retrieval incorrect"

    def test_case_seven(self):
        # ******************************************************************
        # ******************************************************************
        # Case 7:
        # This test case is a HYBRID retrieval.
        # It will ensure that a dependency which contains both datasets and groups can be traversed properly
        # while also being able to process both DATASETS and  without issue
        # ******************************************************************
        # ******************************************************************
        print("--------------------------------------------------------------------------------")
        print("(Case 7): Testing retrieval of both dataset and groups from a dependency")

        # Create Datasets
        list_of_datasets = self.generate_datasets(10)
        # Create Groups
        test_group = self.create_groups(5)

        # Add Group_00 as dependent of Dataset_00
        dataset_00 = self.client.path(path=list_of_datasets[0].path, versionId="current")
        group_dependents_00 = [test_group[0]]
        patched_dataset0 = self.client.add_dependents(dep_container=dataset_00, dep_type="predecessor",
                                                      dep_groups=group_dependents_00)
        # Add 2 datasets (01, 02) to be dependents of Group00
        group_00 = self.client.path(path=test_group[0].path, versionId="current")
        dataset_dependents_01_02 = [list_of_datasets[1], list_of_datasets[2]]
        patched_group_00 = self.client.add_dependents(dep_container=group_00, dep_type="predecessor",
                                                      dep_datasets=dataset_dependents_01_02)
        # Add group_01 to be a dependent of dataset_01
        dataset_01 = self.client.path(path=list_of_datasets[1].path, versionId="current")
        group_dependents_01 = [test_group[1]]
        patched_dataset_01 = self.client.add_dependents(dep_container=dataset_01, dep_type="predecessor",
                                                        dep_groups=group_dependents_01)
        # Add group_02 to be a dependent of dataset_02
        dataset_02 = self.client.path(path=list_of_datasets[2].path, versionId="current")
        group_dependents_02 = [test_group[2]]
        patched_dataset_02 = self.client.add_dependents(dep_container=dataset_02, dep_type="predecessor",
                                                        dep_groups=group_dependents_02)
        # Add 2 datasets(03, 04) to be dependents of group_01
        group_01 = self.client.path(path=test_group[1].path, versionId="current")
        datasets_dependents_03_04 = [list_of_datasets[3], list_of_datasets[4]]
        patched_group_01 = self.client.add_dependents(dep_container=group_01, dep_type="predecessor",
                                                      dep_datasets=datasets_dependents_03_04)
        # Add 2 datasets(05, 06) to be dependents of dataset 3
        dataset_03 = self.client.path(path=list_of_datasets[3].path, versionId="current")
        datasets_dependents_05_06 = [list_of_datasets[5], list_of_datasets[6]]
        patched_dataset_03 = self.client.add_dependents(dep_container=dataset_03, dep_type="predecessor",
                                                        dep_datasets=datasets_dependents_05_06)
        # Add 2 datasets(07, 08) to be dependents of dataset 4
        dataset_04 = self.client.path(path=list_of_datasets[4].path, versionId="current")
        datasets_dependents_07_08 = [list_of_datasets[7], list_of_datasets[8]]
        patched_dataset_04 = self.client.add_dependents(dep_container=dataset_04, dep_type="predecessor",
                                                        dep_datasets=datasets_dependents_07_08)

        # Retrieve the entire dependency tree using the api wrapper.
        root_container = self.client.path(path=patched_dataset0.path, versionId="current")
        dependents = self.client.get_dependents(dep_container=root_container, dep_type="predecessor", max_depth=10,
                                                chunk_size=3)

        all_dependents = []
        expected_dependents = ['depGroup0', 'dataset_1.dat', 'dataset_2.dat',
                               'depGroup1', 'depGroup2', 'dataset_3.dat',
                               'dataset_4.dat', 'dataset_5.dat', 'dataset_6.dat',
                               'dataset_7.dat', 'dataset_8.dat'
                               ]

        print("Dependents =")
        try:
            for item in dependents:
                all_dependents.append(item.name)
                print("\t" + item.name)
        except:
            pass
        print()

        while dependents:
            dependents = (self.client.get_next_dependents(root_container))
            print("Dependents =")
            try:
                for item in dependents:
                    if not (item.name in all_dependents):
                        all_dependents.append(item.name)
                        print("\t" + item.name)
            except:
                pass
            print("")

        assert (all_dependents == expected_dependents), \
            "(Case 7 Failure : Expected dependents don't match returned dependents)"

    def generate_datasets(self, number_to_generate):
        file_path = os.path.abspath("../../../test/data/")
        dataset_return = []

        for x in range(number_to_generate):

            datacat_path_generic = '/testpath/testfolder'  # Directory we are working in
            filename_generic = "dataset_" + str(x) + ".dat"  # Name of dataset to be created
            metadata_generic = Metadata()  # Metadata
            metadata_generic['nIsTest'] = 1
            full_file_generic = file_path + '/' + filename_generic  # ../../../test/data/ + filename

            # Check to make sure the dataset doesnt already exist at the provided path
            if self.client.exists(datacat_path_generic + '/' + filename_generic):
                self.client.rmds(datacat_path_generic + '/' + filename_generic)

            # Use the client to create a new dataset using the params mentioned above
            ds_generic = self.client.mkds(datacat_path_generic, filename_generic, 'JUNIT_TEST', 'junit.test',
                                          versionMetadata=metadata_generic,
                                          resource=full_file_generic,
                                          site='SLAC')
            ds_generic_version_pk = ds_generic.versionPk
            # print("created dataset: ", filename_generic, "(VersionPK = ", ds_generic_version_pk, ")" , ds_generic.path)

            dataset_return.append(ds_generic)
        return dataset_return

    def generate_dependencies(self, list_of_datasets):
        level_counter = 1

        # Level 1
        # print(len(list_of_datasets))
        for dataset in range(len(list_of_datasets) - 1):

            try:
                added_before = self.client.path(path=list_of_datasets[dataset].path, versionId="current")
                dataset_to_Patch = self.client.path(path=list_of_datasets[dataset].path, versionId="current")
                dependents = [list_of_datasets[dataset + 1]]
                add_dependents = self.client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                            dep_datasets=dependents)

                if add_dependents:
                    added_after = self.client.path(path=list_of_datasets[dataset].path, versionId="current")
                    add_dpks = []
                    for dependent in dependents:
                        add_dpks.append(dependent.versionPk)
                    # print("\nLevel: " + str(level_counter))
                    # print("Dependents added:", add_dpks, "\n@ " + list_of_datasets[dataset].path + ";v=current\n")
                    level_counter = level_counter + 1
            except:
                assert False, "dependent addition unsuccessful"

    def generate_dependencies_stress_test(self, list_of_datasets):
        # This will add two dependents per level instead of the original 1 dataset per level.
        # We can use this to test the accuracy of the get methods
        last_level_of_dependents = []
        level_counter = 1

        # Level 1
        # print(len(list_of_datasets))
        for datasetIndex in range(0, len(list_of_datasets) - 1, 2):

            try:
                added_before = self.client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
                dataset_to_Patch = self.client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
                dependents = [list_of_datasets[datasetIndex + 1], list_of_datasets[datasetIndex + 2]]
                last_level_of_dependents.clear()
                for x in dependents:
                    last_level_of_dependents.append(x.versionPk)
                add_dependents = self.client.add_dependents(dep_container=dataset_to_Patch, dep_type="predecessor",
                                                            dep_datasets=dependents)

                if add_dependents:
                    added_after = self.client.path(path=list_of_datasets[datasetIndex].path, versionId="current")
                    add_dpks = []
                    for dependent in dependents:
                        add_dpks.append(dependent.versionPk)

                    # print("\nLevel: " + str(level_counter))
                    # print("Dependents added:", add_dpks, "\n@ " + list_of_datasets[datasetIndex].path + ";v=current\n")
                    level_counter = level_counter + 1

            except:
                assert False, "dependent addition unsuccessful"

        return last_level_of_dependents

    def create_groups(self, number_of_groups):
        list_of_groups = []
        container_path_general = "/testpath/depGroup"

        for x in range(0, number_of_groups):
            container_path = container_path_general + str(x)

            try:
                if self.client.exists(container_path):
                    self.client.rmdir(container_path, type="group")

                self.client.mkgroup(container_path)
                dep_group = self.client.path(path=container_path + ";v=current")
                list_of_groups.append(dep_group)

            except:
                assert False, "Group creation failed"

        return list_of_groups
