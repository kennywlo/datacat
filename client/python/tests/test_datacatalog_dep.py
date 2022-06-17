import unittest
import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata


class DataCatalogDep(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # datacat
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        cls.client = client_from_config(config)

    def test_datacatalog_dependent_creation(self):
        # file/datacatalog path
        file_name = 'test_dc0.6_1.root'
        file_path = os.path.abspath("../../../test/data/")
        datacat_path = '/testpath/testfolder'
        full_file = file_path + '/' + file_name

        # metadata
        metadata = Metadata()
        metadata['nIsTest'] = 1

        if self.client.exists(datacat_path+'/' + file_name):
            self.client.rmds(datacat_path+ '/' + file_name)

        ds = self.client.mkds(datacat_path, file_name, 'JUNIT_TEST','junit.test',
                         versionMetadata=metadata,
                         resource=full_file,
                         site='SLAC')

        file_name2 = 'test_dc0.6_2.root'
        full_file2 = file_path + '/' + file_name2

        # metadata
        metadata = Metadata()
        metadata['nIsTest'] = 1

        dependents = self.client.client_helper.get_dependent_id(ds)
        dep_metadata = {"dependents": str(dependents),
                        "dependentType": "successor"}
        metadata.update(dep_metadata)

        if self.client.exists(datacat_path+'/' + file_name2):
            self.client.rmds(datacat_path+ '/' + file_name2)
        self.client.mkds(datacat_path, file_name2, 'JUNIT_TEST', 'junit.test',
                          versionMetadata=metadata,
                          resource=full_file2,
                          site='SLAC')
        assert self.client.exists(datacat_path+'/' + file_name2)


if __name__ == "__main__":
    unittest.main()