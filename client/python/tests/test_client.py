import unittest
import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

__author__ = 'klo'

''' This test created 3 datasets. The first two are the predecessors of the last one.
'''
class Client(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # datacat
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        cls.client = client_from_config(config)

        # file/datacatalog path
        cls.file_path = '~/data'
        cls.datacat_path = '/testpath'

        # metadata
        cls.metadata = Metadata()
        cls.metadata['nIsTest'] = 1

    def test_dataset_one_creation(self):
        filename = "dataset001.dat"
        if self.client.exists(self.datacat_path + '/' + filename):
            self.client.rmds(self.datacat_path + '/' + filename)
        full_file001 = self.file_path + '/' + filename
        self.client.mkds(self.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                            versionMetadata=self.metadata,
                            resource=full_file001,
                            site='SLAC')
        assert self.client.exists(self.datacat_path + '/' + filename)

    def test_dataset_two_creation(self):
        filename = "dataset002.dat"
        if self.client.exists(self.datacat_path + '/' + filename):
            self.client.rmds(self.datacat_path + '/' + filename)
        full_file002 = self.file_path + '/' + filename
        self.client.mkds(self.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                            versionMetadata=self.metadata,
                            resource=full_file002,
                            site='SLAC')
        assert self.client.exists(self.datacat_path + '/' + filename)

    def test_dataset_three_creation(self):
        filename = "dataset003.dat"
        if self.client.exists(self.datacat_path + '/' + filename):
            self.client.rmds(self.datacat_path + '/' + filename)
        full_file003 = self.file_path + '/' + filename
        self.client.mkds(self.datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                            versionMetadata=self.metadata,
                            resource=full_file003,
                            site='SLAC')
        assert self.client.exists(self.datacat_path + '/' + filename)


if __name__ == "__main__":
    unittest.main()
