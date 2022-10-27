import unittest
import os
import posixpath

from datacat import client_from_config, config_from_file
from datacat.model import Metadata


class DataCatalog(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # datacat
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        cls.client = client_from_config(config)

    def test_datacatalog(self):
        # file/datacatalog path
        file_name = 'test_slac.root'
        file_path = posixpath.abspath('/tmp')
        datacat_path = '/testpath/testfolder'
        full_file = posixpath.join(file_path, file_name)

        # metadata
        metadata = Metadata()
        metadata['nIsTest'] = 1

        if self.client.exists(datacat_path + '/' + file_name):
            self.client.rmds(datacat_path + '/' + file_name)

        ds =self.client.mkds(datacat_path, file_name, 'JUNIT_TEST', 'junit.test',
                         versionMetadata=metadata,
                         resource=full_file,
                         site='SLAC')
        assert self.client.exists(datacat_path + '/' + file_name)
        print(ds)

if __name__ == "__main__":
    unittest.main()