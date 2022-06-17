import unittest
import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


class GroupDependency(unittest.TestCase):
    # datacat
    config_file = os.path.dirname(__file__) + '/config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testFolder'

    containerPathSuc = "/testpath/depGroupSuc"
    containerPathPre = "/testpath/depGroupPre"

    containerPath1 = "/testpath/depGroup1"
    containerPath2 = "/testpath/depGroup2"

    containerPathCustom = "/testpath/customDependents"

    # metadata
    metadata = Metadata()
    ds_metadata = Metadata()
    datasets = []

    @classmethod
    def setUpClass(cls) -> None:
        # deleting any duplicate groups
        try:
            if GroupDependency.client.exists(GroupDependency.containerPathSuc):
                GroupDependency.client.rmdir(GroupDependency.containerPathSuc, type="group")

            if GroupDependency.client.exists(GroupDependency.containerPathPre):
                GroupDependency.client.rmdir(GroupDependency.containerPathPre, type="group")

            if GroupDependency.client.exists(GroupDependency.containerPath1):
                GroupDependency.client.rmdir(GroupDependency.containerPath1, type="group")

            if GroupDependency.client.exists(GroupDependency.containerPath2):
                GroupDependency.client.rmdir(GroupDependency.containerPath2, type="group")

            if GroupDependency.client.exists(GroupDependency.containerPathCustom):
                GroupDependency.client.rmdir(GroupDependency.containerPathCustom, type="group")

        except:
            print("exception caught here")
        GroupDependency.datasets.append(1)
        for i in range(1, 5):
            cls.help_create_dataset(i)

        # ********** DEPENDENCY GROUP DATASETS CREATION STARTS HERE ***************
        # *************************************************************************


        # Putting together a list to be used as predecessor dependents
        dependentsPredecessor = GroupDependency.client.client_helper.get_dependent_id(
            [GroupDependency.datasets[1], GroupDependency.datasets[2]])
        print("\nPredecessor dependents genereated as:\n{}".format(dependentsPredecessor))

        # Putting together a list to be used as successor dependents
        dependentsSuccessor = GroupDependency.client.client_helper.get_dependent_id(
            [GroupDependency.datasets[3], GroupDependency.datasets[4]])
        print("\nSuccessor dependents genereated as:\n{}".format(dependentsSuccessor))

        # Putting together a list of dependents that are located in multiple groups
        dependentsSameDatasetDifferentGroups = GroupDependency.client.client_helper.get_dependent_id(
            [GroupDependency.datasets[1], GroupDependency.datasets[2]])

        # Putting together a list to be used as predecessor dependents
        customDependents = GroupDependency.client.client_helper.get_dependent_id(
            [GroupDependency.datasets[1], GroupDependency.datasets[2], GroupDependency.datasets[3],
             GroupDependency.datasets[4]])
        print("\nCustom dependents genereated as:\n{}".format(customDependents))

        # adding the datasets with their new dependentType to a groups metadata (predecessor)
        dep_metadataPredecessor = {
            "dependents": str(dependentsPredecessor),
            "dependentType": "predecessor"
        }

        # adding the datasets with their new dependentType to a groups metadata (successor)
        dep_metadataSuccessor = {
            "dependents": str(dependentsSuccessor),
            "dependentType": "successor"
        }

        # adding the datasets with their new dependentType to a groups metadata (same datasets different groups)
        dep_metadataSameDatasetDifferentGroups = {
            "dependents": str(dependentsSameDatasetDifferentGroups),
            "dependentType": "predecessor"
        }

        # adding the datasets with their new dependentType to a groups metadata (custom)
        dep_metadataCustomDependents = {
            "dependents": str(customDependents),
            "dependentType": "simulation_XXX"
        }

        GroupDependency.metadata.update(dep_metadataPredecessor)
        depGroupPre = GroupDependency.client.mkgroup(GroupDependency.containerPathPre,
                                                     metadata=GroupDependency.metadata)
        print("\nCreated depGroupPre as:\n{} \nMetadata: {}".format(depGroupPre, dict(GroupDependency.metadata)))

        GroupDependency.metadata.update(dep_metadataSuccessor)
        depGroupSuc = GroupDependency.client.mkgroup(GroupDependency.containerPathSuc,
                                                     metadata=GroupDependency.metadata)
        print("\nCreated depGroupSuc as:\n{} \nMetadata: {}".format(depGroupSuc, dict(GroupDependency.metadata)))

        GroupDependency.metadata.update(dep_metadataSameDatasetDifferentGroups)
        depGroup1 = GroupDependency.client.mkgroup(GroupDependency.containerPath1, metadata=GroupDependency.metadata)
        print("\nCreated depGroup1 as:\n{} \nMetadata: {}".format(dep_metadataSameDatasetDifferentGroups,
                                                                  dict(GroupDependency.metadata)))

        GroupDependency.metadata.update(dep_metadataSameDatasetDifferentGroups)
        depGroup2 = GroupDependency.client.mkgroup(GroupDependency.containerPath2, metadata=GroupDependency.metadata)
        print("\nCreated depGroup2 as:\n{} \nMetadata: {}".format(dep_metadataSameDatasetDifferentGroups,
                                                                  dict(GroupDependency.metadata)))

        GroupDependency.metadata.update(dep_metadataCustomDependents)
        depGroupCustom = GroupDependency.client.mkgroup(GroupDependency.containerPathCustom,
                                                        metadata=GroupDependency.metadata)
        print("\nCreated depGroupCustom as:\n{} \nMetadata: {}".format(dep_metadataCustomDependents,
                                                                       dict(GroupDependency.metadata)))

    @classmethod
    def help_create_dataset(cls, i):
        if i == 1:
            filename = "dataset001_82f24.dat"
        elif i == 2:
            filename = "dataset002_92e56.dat"
        elif i == 3:
            filename = "dataset003_0c89c.dat"
        elif i == 4:
            filename = "dataset004_d8080.dat"

        if GroupDependency.client.exists(GroupDependency.datacat_path + '/' + filename):
            GroupDependency.client.rmds(GroupDependency.datacat_path + '/' + filename)
        # use the client to create dataset001 - DOES NOT initialize dependency metadata
        full_file = GroupDependency.file_path + '/' + filename
        ds = GroupDependency.client.mkds(GroupDependency.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                                         versionMetadata=GroupDependency.ds_metadata,
                                         resource=full_file,
                                         site='SLAC')
        print("\nCreated dataset:\n{}\n{}\nMetadata: {}".format(filename, ds, dict(GroupDependency.ds_metadata)))
        GroupDependency.datasets.append(ds)

    # ********** CONTAINER DEPENDENCY TESTING BEGINS HERE **********
    # **************************************************************
    def test_retrieving_predecessors(self):
        # Case 1.1: base case (predecessors) with versionPK value not specified
        # Retrieves all predecessor datasets linked to the group
        print("\n*****Case 1.1*****")
        print("-----Group with dependents-----")
        try:
            print(GroupDependency.client.path(path='/testpath/depGroupPre;metadata=dependents'))
        except:
            assert False, "Error. search unsuccessful. Case 1.1"

    def test_retrieving_predecessors_with_Pk(self):
        # Case 1.2: base case (predecessors) with one versionPK value specified
        # Retrieves specified predecessor datasets linked to the group
        print("\n*****Case 1.2*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupPre', show="dependents",
                                                         query='dependents in ({})'.format(
                                                             GroupDependency.datasets[1].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 1.2"

    def test_retrieving_predecessors_with_morePks(self):
        # Case 1.3: base case (predecessors) with multiple versionPK values specified
        # Retrieves all predecessor datasets linked to the group
        print("*****Case 1.3*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupPre', show="dependents",
                                                         query='dependents in ({},{})'.format(
                                                             GroupDependency.datasets[1].versionPk,
                                                             GroupDependency.datasets[2].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 1.3"

    def test_retrieving_dependents_predecessor(self):
        # Case 2.1: predecessor with versionPK value not specified
        # Retrieves all predecessor datasets linked to the group
        print("*****Case 2.1*****")
        print("-----Group with dependents-----")
        try:
            print(GroupDependency.client.path(path='/testpath/depGroupPre;metadata=dependents.predecessor'))
        except:
            assert False, "Error. search unsuccessful. Case 2.1"

    def test_retrieving_dependents_predecessor_withPk(self):
        # Case 2.2: predecessor with one versionPK value specified
        # Retrieves specified predecessor datasets linked to the group
        print("\n*****Case 2.2*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupPre', show="dependents.predecessor",
                                                         query='dependents in ({})'.format(
                                                             GroupDependency.datasets[1].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 2.2"

    def test_retrieving_dependents_predecessor_with_morePks(self):
        # Case 2.3: predecessor with multiple versionPK values specified
        # Retrieves specified predecessor datasets linked to the group
        print("*****Case 2.3*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupPre', show="dependents.predecessor",
                                                         query='dependents in ({},{})'.format(
                                                             GroupDependency.datasets[1].versionPk,
                                                             GroupDependency.datasets[2].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 2.3"

    def test_retrieving_dependents_successors(self):
        # Case 3.1: successor with versionPK value not specified
        # Retrieves all successor datasets linked to the group
        print("*****Case 3.1*****")
        print("-----Group with dependents-----")
        try:
            print(GroupDependency.client.path(path='/testpath/depGroupSuc;metadata=dependents.successor'))
        except:
            assert False, "Error. search unsuccessful. Case 3.1"

    def test_retrieving_dependents_successors_withPk(self):
        # Case 3.2: successor with one versionPK value specified
        # Retrieves specified successor datasets linked to the group
        print("\n*****Case 3.2*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupSuc', show="dependents.successor",
                                                         query='dependents in ({})'.format(
                                                             GroupDependency.datasets[3].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 3.2"

    def test_retrieving_dependents_successors_with_morePks(self):
        # Case 3.3: successor with multiple versionPK values specified
        # Retrieves specified successor datasets linked to the group
        print("*****Case 3.3*****")
        print("-----Datasets-----")
        try:
            for dataset in GroupDependency.client.search(target='/testpath/depGroupSuc', show="dependents.successor",
                                                         query='dependents in ({},{})'.format(
                                                             GroupDependency.datasets[3].versionPk,
                                                             GroupDependency.datasets[4].versionPk),
                                                         ignoreShowKeyError=True):
                print(f"Name: %s" % (dataset.name))
                print(f"Path: %s" % (dataset.path))
                print()
        except:
            assert False, "Error. search unsuccessful. Case 3.3\n"

    def test_dependent_groups(self):
        # Case 4: test dependents.groups  which should return the Groups associated with a dependent dataset.
        # Retrieves specified datasets and returns what groups are linked to it
        print("*****Case 4*****")
        print("-----Groups-----")
        try:
            print(
                GroupDependency.client.path(path='/testpath/testFolder/dataset001_82f24.dat;metadata=dependents.groups',
                                            versionId=0))
            print()
        except:
            assert False, "Error. search unsuccessful. Case 4\n"

    def test_custom_field(self):
        # Case 5: testing the custom field for datasets.
        # Retrieves all successor datasets linked to the group
        print("*****Case 5*****")
        print("-----Group with Custom Field-----")
        try:
            print(GroupDependency.client.path(path='/testpath/customDependents;metadata=dependents.simulation_XXX'))
        except:
            assert False, "Error. search unsuccessful. Case 5"

    def test_retrieving_proper_groups(self):
        # Case 6: Testing to ensure that the proper groups are returned when using .search to retrieve from a dependency

        print("\n***** .Search error test case *****")

        # First we will create a new dataset and group.
        # ------------------------------------
        # ---creation of dataset_searchTest---
        # ------------------------------------
        datacat_path_searchTest = '/testpath/testfolder'  # Directory we are working in
        filename_searchTest = "dataset_searchTest.dat"  # Name of dataset to be created
        metadata_searchTest = Metadata()  # Metadata
        metadata_searchTest['nIsTest'] = 1
        full_file_searchTest = GroupDependency.file_path + '/' + filename_searchTest  # ../../../test/data/ + filename

        if GroupDependency.client.exists(datacat_path_searchTest + '/' + filename_searchTest):
            GroupDependency.client.rmds(datacat_path_searchTest + '/' + filename_searchTest)

        ds_searchTest = GroupDependency.client.mkds(datacat_path_searchTest, filename_searchTest, 'JUNIT_TEST',
                                                    'junit.test',
                                                    versionMetadata=metadata_searchTest,
                                                    resource=full_file_searchTest,
                                                    site='SLAC')
        ds001_version_pk = ds_searchTest.versionPk
        print("created dataset: ", filename_searchTest, "(VersionPK = ", ds001_version_pk, ")")

        # ------------------------------------
        # ---creation of group_searchTest---
        # ------------------------------------
        container_path1 = "/testpath/group_searchTest"

        try:
            if GroupDependency.client.exists(container_path1):
                GroupDependency.client.rmdir(container_path1, type="group")

            GroupDependency.client.mkgroup(container_path1)
            dep_group_searchTest = GroupDependency.client.path(path='/testpath/group_searchTest', versionId='current')
            print("created group: ", dep_group_searchTest.name, "(pk = ", dep_group_searchTest.pk, ")")
        except:
            assert False, "Group creation failed"

        # Now we will create the needed dependency. Group will be a dependent of dataset

        print("\nAdding group dependents")
        added_before = GroupDependency.client.path(path=ds_searchTest.path, versionId="current")
        print("\t", added_before.versionMetadata)

        update_dependents = [dep_group_searchTest]
        GroupDependency.client.add_dependents(dep_container=ds_searchTest, dep_type="predecessor",
                                              dep_groups=update_dependents)

        added_after = GroupDependency.client.path(path=ds_searchTest.path, versionId="current")
        print("\t", added_after.versionMetadata)

        # Now we will use .search to try and retrieve the dependent group using "dependency.groups" and a query.
        # This will be the issue as the return value is empty

        searchResults = GroupDependency.client.search(target=ds_searchTest.path,
                                                      show="dependency.groups",
                                                      containerFilter='dependentGroups in ({})'.format(
                                                          dep_group_searchTest.pk),
                                                      ignoreShowKeyError=True)

        print("\nReturn value of client.search call:", searchResults)

        searchResults = GroupDependency.client.search(target=ds_searchTest.path,
                                                      show="dependency.groups",
                                                      containerFilter='dependentGroups in ({})'.format(
                                                          dep_group_searchTest.pk),
                                                      ignoreShowKeyError=True)

        print("\nReturn value of client.search call:", searchResults)

        expectedValue = dep_group_searchTest.pk
        assert (searchResults[0].pk == expectedValue), "Expected value not equal to returned value"
