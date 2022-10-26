import unittest
import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


def create_datasets(case, num, has_metadata=True):
    # datacat
    config_file = os.path.dirname(__file__) + '/config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    file_path = os.path.abspath("../../../test/data/")
    print("****** CASE {} DATASETS CREATION BEGIN ******\n".format(case))

    dataset_nums = num
    dataset_list = []

    for ds_num in range(dataset_nums):
        datacat_path = '/testpath/testfolder'  # Directory we are working in
        filename = "dataset00{}_{}.dat".format(ds_num + 1, case)  # Name of dataset to be created
        metadata = None

        if has_metadata:
            metadata = Metadata()  # Metadata
            metadata['nIsTest'] = 1
        else:
            filename = "dataset00{}_{}_no_vmd".format(ds_num + 1, case)

        full_file = file_path + '/' + filename  # ../../../test/data/ + filename

        # Check to make sure the dataset doesnt already exist at the provided path
        if client.exists(datacat_path + '/' + filename):
            client.rmds(datacat_path + '/' + filename)

        # Use the client to create a new dataset using the params mentioned above
        ds = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                         versionMetadata=metadata,
                         resource=full_file,
                         site='SLAC')
        ds_version_pk = ds.versionPk
        print("created dataset: ", filename, "(VersionPK = ", ds_version_pk, ")")
        dataset_list.append(ds)

    print("\n")
    return dataset_list


def create_groups(case, num):
    # datacat
    config_file = os.path.dirname(__file__) + '/config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)
    print("\n******CASE {} GROUPS CASE CREATION BEGIN ******\n".format(case))

    group_nums = num
    group_list = []

    for gp_num in range(group_nums):

        containerPath = "/testpath/depGroup{}_{}".format(gp_num + 1, case)
        try:
            if client.exists(containerPath):
                client.rmdir(containerPath, type="group")

            client.mkgroup(containerPath)
            client.path(path='/testpath/depGroup{}_{}'.format(gp_num + 1, case))
            group = client.path(path='{}'.format(containerPath))
            print("created group:", group.name, "(Pk = {})".format(group.pk))

            group_list.append(group)

        except:
            assert False, "Group creation failed"

    return group_list


class ApiWrapper(unittest.TestCase):

    case1_datasets = create_datasets(case=1, num=3)
    dataset001_1 = case1_datasets[0]
    dataset002_1 = case1_datasets[1]
    dataset003_1 = case1_datasets[2]

    case1_datasets_no_vmd = create_datasets(1, 1, has_metadata=False)
    dataset001_1_no_vmd = case1_datasets_no_vmd[0]

    case2_datasets = create_datasets(2, 3)
    dataset001_2 = case2_datasets[0]
    dataset002_2 = case2_datasets[1]
    dataset003_2 = case2_datasets[2]

    case3_datasets = create_datasets(3, 3)
    dataset001_3 = case3_datasets[0]
    dataset002_3 = case3_datasets[1]
    dataset003_3 = case3_datasets[2]

    case4_datasets = create_datasets(4, 4)
    dataset001_4 = case4_datasets[0]
    dataset002_4 = case4_datasets[1]
    dataset003_4 = case4_datasets[2]
    dataset004_4 = case4_datasets[3]

    case5_datasets = create_datasets(5, 3)
    dataset001_5 = case5_datasets[0]
    dataset002_5 = case5_datasets[1]
    dataset003_5 = case5_datasets[2]

    # Datasets for Case2 removal starting as case 6
    case6_datasets = create_datasets(6, 3)
    dataset001_6 = case6_datasets[0]
    dataset002_6 = case6_datasets[1]

    case3_groups = create_groups(3, 2)
    group1_3 = case3_groups[0]
    group2_3 = case3_groups[1]

    case4_groups = create_groups(4, 5)
    group1_4 = case4_groups[0]
    group2_4 = case4_groups[1]
    group3_4 = case4_groups[2]
    group4_4 = case4_groups[3]
    group5_4 = case4_groups[4]

    case5_groups = create_groups(5, 3)
    group1_5 = case5_groups[0]
    group2_5 = case5_groups[1]
    group3_5 = case5_groups[2]

    # Groups for Case2 removal starting as case 6

    case6_groups = create_groups(6, 4)
    group1_6 = case6_groups[0]
    group2_6 = case6_groups[1]
    group3_6 = case6_groups[2]
    group4_6 = case6_groups[3]

    enable_assertion = True
    store_expected = None

    @classmethod
    def setUpClass(cls) -> None:
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        cls.client = client_from_config(config)

    @classmethod
    def check_match(cls, expect, actual):
        """
        Check if the comma delimated string contains the same elements.
        :param expect: expected comma delimited string
        :param actual: the returned comma delimited string
        :return: If the numbers present are the same but just in different ordered
        return true, otherwise, return false.
        """
        expect_list = expect.split(",")
        actual_list = actual.split(",")

        if len(expect_list) != len(actual_list):
            return False

        return set(expect_list) == set(actual_list)

    "******* add_dependents() TESTING BEGINS *******\n"

    """
    ****************************************************
    *** The following are dataset as container tests ***
    ****************************************************
    """

    def test_add_case_one(self):
        try:
            added_before = self.client.path(path=self.dataset001_1.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_1.path, versionId="current")
            dependents = [self.dataset002_1, self.dataset003_1]
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch, dep_type="predecessor",
                                                   dep_datasets=dependents)
            if add_dependents:
                added_after = self.client.path(path=self.dataset001_1.path, versionId="current")
                add_dpks = self.client.client_helper.get_dependent_id(dependents)
                print("DATASET DEPENDENTS TO UPDATE:", add_dpks)
                print("Case 1.1 dependent addition successful:")
                print("** add datasets to dataset container **")
                print("CONTAINER DATASET NAME:", dataset_to_patch.name)
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.1 dependent addition unsuccessful"

        try:
            dataset_to_patch = self.client.path(path=self.dataset001_1_no_vmd.path, versionId="current")
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch, dep_type="predecessor",
                                                   dep_datasets=dependents)
            if add_dependents:
                added_after = self.client.path(path=self.dataset001_1_no_vmd.path, versionId="current")
                print("Case 1.2 dependent addition successful: ")
                print("OLD METADATA OUTPUT:", None)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.2 dependency creation unsuccessful"

    def test_add_case_two(self):
        # Case 2.1 dataset has dependency metadata of same dependent type -> add new dependents to same dependent type
        try:
            added_before = self.client.path(path=self.dataset001_1.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_1.path, versionId="current")
            update_dependents = [self.dataset001_2, self.dataset002_2]
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch, dep_type="predecessor",
                                                   dep_datasets=update_dependents)
            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependents)
                print("Case 2.1 dependent update successful:")
                print("DATASET DEPENDENTS TO UPDATE:", update_dpks)
                added_after = self.client.path(path=self.dataset001_1.path, versionId="current")
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 2.1 dependent addition unsuccessful"

        # Case 2.2 dataset has dependency metadata of different dependent type -> add dependents to new dependent type
        try:
            added_before = self.client.path(path=self.dataset001_1.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_1.path, versionId="current")
            update_dependents = [self.dataset003_2]
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch, dep_type="successor",
                                                   dep_datasets=update_dependents)
            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependents)
                print("Case 2.2 dependent update successful:")
                print("CONTAINER DATASET NAME:", dataset_to_patch.name)
                print("DATASET DEPENDENTS TO UPDATE:", update_dpks)
                added_after = self.client.path(path=self.dataset001_1.path, versionId="current")
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 2.2 dependent addition unsuccessful"

    def test_add_case_three(self):
        # Case 3.1 add groups as dependent to dataset container
        try:
            added_before = self.client.path(path=self.dataset001_3.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_3.path, versionId="current")
            update_dependents = [self.group1_3]
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch, dep_type="successor",
                                                   dep_groups=update_dependents)
            added_after = self.client.path(path=dataset_to_patch.path + ";versionMetadata=dependents",
                                      versionId=dataset_to_patch.versionId)
            expected = {'successor.group': str(update_dependents[0].pk)}
            added_after = dict(added_after)

            if self.enable_assertion:
                assert expected['successor.group'] == added_after['successor.group'], \
                    "Case 3.1 dataset dependent addition result is not as expected: {}.\nExpected: {}".format(added_after,
                                                                                                              expected)

            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(update_dependents)
                print("Case 3.1 dependent update successful:")
                print("CONTAINER DATASET NAME:", dataset_to_patch.name)
                print("GROUP DEPENDENTS TO UPDATE:", update_gpks)
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")
        except:
            assert False, "Case 3.1 dependent addition unsuccessful"

        # Case 3.2 add groups AND datasets as dependent to dataset container
        try:
            added_before = self.client.path(path=self.dataset002_3.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset002_3.path, versionId="current")
            update_dependent_groups = [self.group2_3]
            update_dependent_datasets = [self.dataset003_3]
            add_dependents = self.client.add_dependents(dep_container=dataset_to_patch,
                                                   dep_type="successor",
                                                   dep_datasets=update_dependent_datasets,
                                                   dep_groups=update_dependent_groups)

            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependent_datasets)
                update_gpks = self.client.client_helper. get_dependent_id(update_dependent_groups)
                expected = {'successor.dataset': '{}'.format(update_dpks[0]),
                            'successor.group': '{}'.format(update_gpks[0])}
                added_after = self.client.path(path=dataset_to_patch.path + ";versionMetadata=dependents",
                                          versionId=dataset_to_patch.versionId)
                added_after = dict(added_after)
                if self.enable_assertion:
                    assert expected['successor.dataset'] == added_after['successor.dataset'] and \
                           expected['successor.group'] == added_after['successor.group'], "Case 3.2 dataset dependent " \
                                                                                          "addition result is not as expected: {}.\nExpected: {}".format(
                        added_after, expected)
                print("Case 3.2 dependent update successful:")
                print("CONTAINER DATASET NAME:", dataset_to_patch.name)
                print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")
        except:
            assert False, "Case 3.2 dependent addition unsuccessful"

    """
    **************************************************
    *** The following are group as container tests ***
    **************************************************
    """

    def test_add_case_four_and_five(self):
        # Case 4.1 Attach dataset to group container
        try:
            added_before = self.client.path(path='/testpath/depGroup1_4')
            group_to_patch = self.client.path(path='/testpath/depGroup1_4')
            update_dependents = [self.dataset001_4, self.dataset002_4]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                   dep_datasets=update_dependents)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependents)
                expected = {'predecessor.dataset': '{},{}'.format(update_dpks[0], update_dpks[1])}
                added_after = self.client.path(path='/testpath/depGroup1_4;metadata=dependents')
                added_after = dict(added_after)

                if self.enable_assertion:
                    assert added_after['predecessor.dataset'] == expected['predecessor.dataset'], \
                        "Case 4.1 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 4.1 group dependent addition successful:")
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 4.1 group dependent addition unsuccessful"

        # Case 4.2 Attach group to group container
        try:
            added_before = self.client.path(path='/testpath/depGroup2_4')
            group_to_patch = self.client.path(path='/testpath/depGroup2_4')
            update_dependents = [self.group3_4]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                   dep_groups=update_dependents)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(update_dependents)
                added_after = self.client.path(path='/testpath/depGroup2_4;metadata=dependents')
                added_after = dict(added_after)
                expected = {
                    'predecessor.group': '{}'.format(update_gpks[0])
                }
                if self.enable_assertion:
                    assert added_after['predecessor.group'] == expected['predecessor.group'], \
                        "Case 4.2 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 4.2 group dependent addition successful:")
                print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 4.2 group dependent addition unsuccessful"

        # Case 4.3 Attach dataset AND group to a group container
        try:

            added_before = self.client.path(path='/testpath/depGroup3_4')
            group_to_patch = self.client.path(path='/testpath/depGroup3_4')
            update_dependent_groups = [self.group4_4]
            update_dependent_datasets = [self.dataset003_4, self.dataset004_4]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch,
                                                   dep_type="predecessor",
                                                   dep_groups=update_dependent_groups,
                                                   dep_datasets=update_dependent_datasets)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(update_dependent_groups)
                update_dpks = self.client.client_helper.get_dependent_id(update_dependent_datasets)
                added_after = self.client.path(path='/testpath/depGroup3_4;metadata=dependents')
                added_after = dict(added_after)
                expected = {'predecessor.dataset': '{},{}'.format(update_dpks[0], update_dpks[1]),
                            'predecessor.group': '{}'.format(update_gpks[0])
                            }
                self.store_expected = expected

                if self.enable_assertion:
                    assert added_after['predecessor.dataset'] == expected['predecessor.dataset'] and \
                           added_after['predecessor.group'] == expected['predecessor.group'], \
                        "Case 4.3 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 4.3 group dependent addition successful:")
                print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 4.3 group dependent addition unsuccessful"

        # Case 5.1 Patch dataset dependents in group container
        try:
            added_before = self.client.path(path='/testpath/depGroup1_4;v=current')
            group_to_patch = self.client.path(path='/testpath/depGroup1_4;v=current')
            update_dependents = [self.dataset001_5, self.dataset002_5]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                   dep_datasets=update_dependents)

            if hasattr(added_before, "metadata"):
                group_md = dict(added_before.metadata)

            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependents)
                added_after = self.client.path(path='/testpath/depGroup1_4;metadata=dependents')
                added_after = dict(added_after)
                expected_predecessor_dataset_str = '{}'.format(added_before.metadata['predecessor.dataset']) + ',' \
                                                   + '{},{}'.format(update_dpks[0], update_dpks[1])

                predecessor_check = self.check_match(expected_predecessor_dataset_str, added_after['predecessor.dataset'])

                if self.enable_assertion:
                    assert predecessor_check and \
                           'successor.dataset' not in added_after, \
                        "Case 5.1 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, self.store_expected)

                print("Case 5.1 group dependent addition successful:")
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 5.1 group dependent addition unsuccessful"

        # Case 5.2 Patch group dependents in group container
        try:
            added_before = self.client.path(path='/testpath/depGroup2_4')
            group_to_patch = self.client.path(path='/testpath/depGroup2_4')
            update_dependents = [self.group1_5]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                   dep_groups=update_dependents)

            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata

            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(update_dependents)
                expected = {'predecessor.group': '{},{}'.format(group_md['predecessor.group'], update_dpks[0])}
                expected_v2 = {'predecessor.group': '{},{}'.format(update_dpks[0], group_md['predecessor.group'])}
                added_after = self.client.path(path='/testpath/depGroup2_4;metadata=dependents')
                added_after = dict(added_after)
                if self.enable_assertion:
                    assert (added_after['predecessor.group'] == expected['predecessor.group']
                            or added_after['predecessor.group'] == expected_v2['predecessor.group']) and \
                           'successor.dataset' not in added_after, \
                        "Case 5.2 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 5.2 group dependent addition successful:")
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 5.2 group dependent addition unsuccessful"

        # Case 5.3 patch dataset AND group dependents in group container
        try:

            added_before = self.client.path(path='/testpath/depGroup3_4')
            group_to_patch = self.client.path(path='/testpath/depGroup3_4')
            update_dependent_groups = [self.group2_4]
            update_dependent_datasets = [self.dataset003_5]
            add_dependents = self.client.add_dependents(dep_container=group_to_patch,
                                                   dep_type="predecessor",
                                                   dep_groups=update_dependent_groups,
                                                   dep_datasets=update_dependent_datasets)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata

            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(update_dependent_groups)
                update_dpks = self.client.client_helper.get_dependent_id(update_dependent_datasets)
                added_after = self.client.path(path='/testpath/depGroup3_4;metadata=dependents')
                added_after = dict(added_after)
                expected_predecessor_dataset_str = '{}'.format(group_md['predecessor.dataset']) + ',' + '{}'.format(update_dpks[0])
                expected_predecessor_group_str = '{}'.format(update_gpks[0]) + ',' + '{}'.format(group_md['predecessor.group'])

                if self.enable_assertion:
                    predecessor_group_check = self.check_match(expected_predecessor_group_str, added_after['predecessor.group'])
                    predecessor_dataset_check = self.check_match(expected_predecessor_dataset_str, added_after['predecessor.dataset'])

                    assert predecessor_group_check and \
                           predecessor_dataset_check , \
                        "Case 5.3 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 5.3 group dependent addition successful:")
                print("DEPENDENT GROUPS TO BE ADDED:", update_gpks)
                print("DEPENDENT DATASETS TO BE ADDED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 5.3 group dependent addition unsuccessful"

    """ ============= remove_dependents() testing starts here =============
    # ==================================================================="""
    "\n******* remove_dependents() TESTING BEGINS *******\n"

    def test_remove_case_one(self):
        # Case 1.1 remove user requested dependency version metadata
        try:
            added_before = self.client.path(path=self.dataset001_1.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_1.path, versionId="current")
            remove_dependents = [self.dataset002_1]
            add_dependents = self.client.remove_dependents(dep_container=dataset_to_patch, dep_type="predecessor",
                                                      dep_datasets=remove_dependents)

            if add_dependents:
                remove_dpks = []
                for dependent in remove_dependents:
                    remove_dpks.append(dependent.versionPk)
                added_after = self.client.path(path=self.dataset001_1.path, versionId="current")

                expected_predecessor_dataset_l = added_before.versionMetadata['predecessor.dataset'].split(",")
                remove_dpks_s = str(remove_dpks[0])
                if remove_dpks_s in expected_predecessor_dataset_l:
                    expected_predecessor_dataset_l.remove(remove_dpks_s)
                expected_predecessor_dataset_str = ",".join(expected_predecessor_dataset_l)

                expected = {
                    'successor.dataset': added_before.versionMetadata['successor.dataset'],
                    'predecessor.dataset': expected_predecessor_dataset_str
                }
                if self.enable_assertion:
                    assert added_after.versionMetadata['predecessor.dataset'] == expected['predecessor.dataset'] and \
                           added_after.versionMetadata['successor.dataset'] == expected['successor.dataset'], \
                        "Case 1.1 dependent removal result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)


                print("Dataset dependents to be removed:", remove_dpks)
                print("Case 1.1 dependent removal successful:")
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.1 dependents removal unsuccessful"

        # Case 1.2 remove all dependency version metadata
        try:
            added_before = self.client.path(path=self.dataset001_1.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_1.path, versionId="current")
            remove_dependents = [self.dataset003_1, self.dataset001_2, self.dataset002_2]
            add_dependents = self.client.remove_dependents(dep_container=dataset_to_patch, dep_type="predecessor",
                                                      dep_datasets=remove_dependents)
            if add_dependents:
                remove_dpks = self.client.client_helper.get_dependent_id(remove_dependents)
                added_after = self.client.path(path=self.dataset001_1.path, versionId="current")
                print("Dataset dependents to be removed:", remove_dpks)
                if self.enable_assertion:
                    if "predecessor.dataset" in added_after.versionMetadata:
                        assert False, "Dependents are not completely removed. " \
                                      "{} remains in versionMetadata." \
                            .format(added_after.versionMetadata["predecessor.dataset"])

                print("Case 1.2 dependent removal successful:")
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.2 dependents removal unsuccessful"

        # Case 1.3 remove group from dataset container
        try:
            added_before = self.client.path(path=self.dataset001_3.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset001_3.path, versionId="current")
            remove_dependents = [self.group1_3]
            add_dependents = self.client.remove_dependents(dep_container=dataset_to_patch, dep_type="successor",
                                                      dep_groups=remove_dependents)

            if add_dependents:
                remove_gpks = []
                for dependent in remove_dependents:
                    remove_gpks.append(dependent.pk)
                added_after = self.client.path(path=self.dataset001_3.path, versionId="current")
                expected = {
                    'dependencyName': '/testpath/testfolder/dataset001_3.dat;v=0'
                }
                if self.enable_assertion:
                    assert 'predecessor.group' not in added_after.versionMetadata and \
                           'successor.dataset' not in added_after.versionMetadata, \
                        "Case 1.3 dependent removal result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after.versionMetadata, expected)

                print("Case 1.3 dependent removal successful:")
                print("DEPENDENT GROUPS TO BE REMOVED:", remove_gpks)
                print("CONTAINER DATASET NAME:", added_before.name)
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.3 dependents removal unsuccessful"

        # Case 1.4 remove group AND dataset from dataset container
        try:
            added_before = self.client.path(path=self.dataset002_3.path, versionId="current")
            dataset_to_patch = self.client.path(path=self.dataset002_3.path, versionId="current")
            remove_dependent_datasets = [self.dataset003_3]
            remove_dependent_groups = [self.group2_3]
            add_dependents = self.client.remove_dependents(dep_container=dataset_to_patch,
                                                      dep_type="successor",
                                                      dep_datasets=remove_dependent_datasets,
                                                      dep_groups=remove_dependent_groups)

            if add_dependents:
                remove_gpks = []
                remove_dpks = []
                for dependent in remove_dependent_groups:
                    remove_gpks.append(dependent.pk)
                for dependent in remove_dependent_datasets:
                    remove_dpks.append(dependent.versionPk)

                added_after = self.client.path(path=self.dataset002_3.path, versionId="current")
                expected = {
                    'dependencyName': '/testpath/testfolder/dataset002_3.dat;v=0'
                }
                if self.enable_assertion:
                    assert 'successor.group' not in added_after.versionMetadata and \
                           'successor.dataset' not in added_after.versionMetadata, \
                        "Case 1.4 dependent removal result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after.versionMetadata, expected)

                print("Case 1.4 dependent removal successful:")
                print("DEPENDENT GROUPS TO BE REMOVED:", remove_gpks)
                print("DEPENDENT DATASETS TO BE REMOVED:", remove_dpks)
                print("CONTAINER DATASET NAME:", added_before.name)
                print("OLD METADATA OUTPUT:", added_before.versionMetadata)
                print("UPDATED METADATA OUTPUT:", added_after.versionMetadata)
                print("\n")
        except:
            assert False, "Case 1.4 dependents removal unsuccessful"

    def test_remove_case_two(self):
        # Case 2.1 remove dataset from group container
        try:
            added_before = self.client.path(path='/testpath/depGroup1_4')
            group_to_patch = self.client.path(path='/testpath/depGroup1_4')
            remove_dependents = [self.dataset001_4]
            add_dependents = self.client.remove_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                      dep_datasets=remove_dependents)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_dpks = self.client.client_helper.get_dependent_id(remove_dependents)

                expected_predecessor_dataset_l = group_md['predecessor.dataset'].split(",")
                update_dpks_s = str(update_dpks[0])
                if update_dpks_s in expected_predecessor_dataset_l:
                    expected_predecessor_dataset_l.remove(update_dpks_s)
                expected_predecessor_dataset_str = ",".join(expected_predecessor_dataset_l)

                expected = {'predecessor.dataset': expected_predecessor_dataset_str}

                added_after = self.client.path(path='/testpath/depGroup1_4;metadata=dependents')
                added_after = dict(added_after)
                if self.enable_assertion:
                    assert sorted(added_after['predecessor.dataset'].split(",")) == sorted(expected['predecessor.dataset'].split(",")), \
                                "Case 2.1 group dependent addition result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 2.1 group dependent removal successful:")
                print("DEPENDENT DATASETS TO BE REMOVE:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 2.1 group dependent removal unsuccessful"

        # Case 2.2 remove group from group container
        try:
            container_group = self.client.path(path='/testpath/depGroup2_6')
            self.client.add_dependents(dep_container=container_group, dep_type="predecessor", dep_groups=[self.group1_6])
            added_before = self.client.path(path='/testpath/depGroup2_6')
            group_to_patch = self.client.path(path='/testpath/depGroup2_6')
            remove_dependents = [self.group1_6]
            add_dependents = self.client.remove_dependents(dep_container=group_to_patch, dep_type="predecessor",
                                                      dep_groups=remove_dependents)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(remove_dependents)
                added_after = self.client.path(path='/testpath/depGroup2_6;metadata=dependents')
                added_after = dict(added_after)

                if self.enable_assertion:
                    expected = {}
                    assert 'predecessor.group' not in added_after and \
                           'successor.group' not in added_after and \
                           'predecessor.dataset' not in added_after and \
                           'successor.dataset' not in added_after, \
                        "Case 2.2 group dependent removal result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 2.2 group dependent removal successful:")
                print("DEPENDENT GROUPS TO BE REMOVE:", update_gpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 2.2 group dependent removal unsuccessful"

        # Case 2.3 remove datasets AND group from group container
        try:
            container_group = self.client.path(path='/testpath/depGroup4_6')
            self.client.add_dependents(dep_container=container_group,
                                  dep_type="predecessor",
                                  dep_groups=[self.group3_6],
                                  dep_datasets=[self.dataset002_6])

            added_before = self.client.path(path='/testpath/depGroup4_6')
            group_to_patch = self.client.path(path='/testpath/depGroup4_6')
            remove_dependent_groups = [self.group3_6]
            remove_dependent_datasets = [self.dataset002_6]
            add_dependents = self.client.remove_dependents(dep_container=group_to_patch,
                                                      dep_type="predecessor",
                                                      dep_groups=remove_dependent_groups,
                                                      dep_datasets=remove_dependent_datasets)
            group_md = None
            if hasattr(added_before, "metadata"):
                group_md = added_before.metadata
            if add_dependents:
                update_gpks = self.client.client_helper.get_dependent_id(remove_dependent_groups)
                update_dpks = self.client.client_helper.get_dependent_id(remove_dependent_datasets)
                added_after = self.client.path(path='/testpath/depGroup4_6;metadata=dependents')
                added_after = dict(added_after)

                if self.enable_assertion:
                    expected = {}
                    assert 'predecessor.group' not in added_after and \
                           'predecessor.dataset' not in added_after and \
                           'successor.group' not in added_after and \
                           'successor.dataset' not in added_after, \
                        "Case 2.3 group dependent removal result is not as expected: {}.\n" \
                        "Expected: {}".format(added_after, expected)

                print("Case 2.3 group dependent removal successful:")
                print("DEPENDENT GROUPS TO BE REMOVED:", update_gpks)
                print("DEPENDENT DATASETS TO BE REMOVED:", update_dpks)
                print("CONTAINER GROUP NAME:", added_before.name)
                print("CONTAINER GROUP OLD METADATA:", group_md)
                print("UPDATED METADATA OUTPUT:", added_after)
                print("\n")

        except:
            assert False, "Case 2.3 group dependent removal unsuccessful"


if __name__ == "__main__":
    unittest.main()
