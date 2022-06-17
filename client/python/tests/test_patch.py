import unittest
import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata, DatasetLocation


class Patch(unittest.TestCase):
    # datacat
    config_file = os.path.dirname(__file__) + '/config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testfolder'

    # ********** DATASET CREATION STARTS HERE **********
    # initializing metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1
    # initializing dependency metadata
    dp_metadata = Metadata()
    metadata['nIsTest'] = 1
    datasets = [0, 1, 2, 3, 4]

    @classmethod
    def setUpClass(cls) -> None:
        for i in range(1, 4):
            cls.help_create_dataset(i, Patch.client)

        Patch.container_path_predecessor = "/testpath/dependencyGroup"

        # Check to see if group already exists at the path, if it does... delete old group
        try:
            if Patch.client.exists(Patch.container_path_predecessor):
                Patch.client.rmdir(Patch.container_path_predecessor, type="group")
        except:
            print("exception caught here")

        # Use the client to create the new group alongside its new dependency metadata
        Patch.group = Patch.client.mkgroup(Patch.container_path_predecessor, metadata=Patch.metadata)
        print("\nCreated New Group as:\n{}".format(Patch.group))

    def test_dataset_patching(self):
        # Printing the dependency relations before patch
        print_dependency_before(Patch.client)

        # Retrieving Dataset to patch
        datasetToPatch = Patch.client.path(path="/testpath/testfolder/dataset002_92e56.dat;v=current")

        # Creating the dependency information
        dependency_metadata = {"dependents": str(Patch.datasets[1][0].versionPk),
                               # VersionPKs of the dependent datasets
                               "dependentType": "predecessor"}  # [predecessor], [successor], [custom]
        # Updating the metadata
        datasetToPatch.versionMetadata.update(dependency_metadata)

        # Patching the updated metadata
        ds002_return = Patch.client.patchds(path="/testpath/testfolder/dataset002_92e56.dat", dataset=datasetToPatch)

        # Printing the dependency relations after patch
        print_dependency_after(Patch.client)

    # **************************************************
    # **************************************************
    # ********** DIRECTORY PATCHING STARTS HERE ********
    # **************************************************
    # **************************************************
    def test_directory_patch(self):
        metadata = Metadata()

        dep_metadataPredecessor = {
            "dependents": str(Patch.datasets[1][0].versionPk),
            "dependentType": "predecessor"
        }

        metadata.update(dep_metadataPredecessor)
        Patch.group.metadata.update(dep_metadataPredecessor)

        returnedGroup = Patch.client.patchdir(path=Patch.container_path_predecessor, container=Patch.group,
                                              type="group")
        print(returnedGroup)

    # *********** Testing mkloc ***********
    def test_mkloc(self):
        print("client.mkloc test begins")
        ds003 = Patch.client.mkloc(path="/testpath/testfolder/dataset003.dat", site="OSN",
                                   resource=Patch.datasets[2][1])
        ds003_return = Patch.client.path(path="/testpath/testfolder/dataset003.dat", versionId="current")
        print("client.mkloc test result:", ds003)
        print("dataset003 location:", ds003_return.locations)

    # *********** Testing creating dataset with 2 locations ***********
    def test_dataset_with_2_locations(self):
        filename = "dataset004.dat"
        if Patch.client.exists(Patch.datacat_path + '/' + filename):
            Patch.client.rmds(Patch.datacat_path + '/' + filename)

        full_file004 = Patch.file_path + '/' + filename

        location1 = DatasetLocation(resource=full_file004, site='SLAC')
        location2 = DatasetLocation(resource=full_file004, site='OSN')
        ds004 = Patch.client.mkds(Patch.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                                  versionMetadata=Patch.metadata,
                                  locations=[location1, location2])

        print("locations creation result:", ds004)
        ds004_return = Patch.client.path(path="/testpath/testfolder/dataset004.dat", versionId="current")
        print("dataset004 location:", ds004_return.locations)

    @classmethod
    def help_create_dataset(cls, i, client):
        if i == 1:
            filename = "dataset001_82f24.dat"
            purpose = "Utility"
        elif i == 2:
            filename = "dataset002_92e56.dat"
            purpose = "Utility"
        elif i == 3:
            filename = "dataset003.dat"
            purpose = "mkloc"

        if client.exists(Patch.datacat_path + '/' + filename):
            client.rmds(Patch.datacat_path + '/' + filename)
        # use the client to create dataset001 - DOES NOT initialize dependency metadata
        full_file = Patch.file_path + '/' + filename
        ds = client.mkds(Patch.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                         versionMetadata=Patch.metadata,
                         resource=full_file,
                         site='SLAC')
        print("\ncreated dataset: ", filename, "(For ", purpose, " Testing) (VersionPK = ", ds.versionPk, ")")
        Patch.datasets[i] = [ds, full_file]


def print_dependency_before(client):
    # Printing dependency relations prior to the patch
    print("\nBefore Patch:")
    print("-----------------------------------------------------------------------")
    print("Datasets containing dependency of type predecessor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" % (dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" % (dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"

    print("\nDatasets containing dependency of type successor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" % (dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" % (dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"
    print("-----------------------------------------------------------------------")


def print_dependency_after(client):
    # Printing dependency relations prior to the patch
    print("\nBefore Patch:")
    print("-----------------------------------------------------------------------")
    print("Datasets containing dependency of type predecessor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" % (dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" % (dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"

    print("\nDatasets containing dependency of type successor:")
    try:
        for dataset in client.search(target='/testpath/testfolder', show="dependents.successor",
                                     ignoreShowKeyError=True):
            try:
                print(f"Name: %s metadata: %s" % (dataset.name, dict(dataset.metadata)))
            except:
                print(f"Name: %s" % (dataset.name))
    except:
        assert False, "Error. search unsuccessful. Before Patch"
    print("-----------------------------------------------------------------------")
