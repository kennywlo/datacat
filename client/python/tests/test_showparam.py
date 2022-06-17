import unittest
import os
from datacat import client_from_config, config_from_file


class ShowParam(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config_file = os.path.dirname(__file__) + '/config_srs.ini'
        config = config_from_file(config_file)
        cls.client = client_from_config(config)

    def test_query_with_flag(self):
        # Case 1 Valid query and ignoreShowKeyError flag is present
        try:
            self.client.search(target='/testpath/testfolder', show='nIsTest', ignoreShowKeyError=True)
            print("Query passed as expected (Case 1: valid query and ignoreShowKeyError flag is present)")
        except:
            assert False, "Error. Query should be valid (Case 1)."

    def test_query_without_flag(self):
        # Case 2 Valid query and ignoreShowKeyError flag is not present
        try:
            self.client.search(target='/testpath/testfolder', show='nIsTest', ignoreShowKeyError=False)
            assert False, "Error. Query should be valid (Case 2)."
        except:
            print("Query passed as expected (Case 2: Valid query and ignoreShowKeyError flag is not present)")

    def test_invalid_query_with_flag(self):
        # Case 3 Invalid query and ignoreShowKeyError flag is present
        try:
            self.client.search(target='/testpath/testfolder', show='FakeKey', ignoreShowKeyError=True)
            print("Query passed as expected (Case 3: Invalid query and ignoreShowKeyError flag is present)")
        except:
            assert False, "Error. Query should be valid (Case 3)."

    def test_invalid_query_without_flag(self):
        # Case 4 Invalid query and ignoreShowKeyError flag is not present
        try:
            self.client.search(target='/testpath/testfolder', show='FakeKey', ignoreShowKeyError=False)
            assert False, "Error. Query should had failed (Case 4)."
        except:
            print("Query failed as expected (Case 4: Invalid query and ignoreShowKeyError flag is not present).")

    def test_query_content(self):
        # Case 5 * query with content printed
        try:
            for dataset in self.client.search(target='/testpath/testfolder', show='*', ignoreShowKeyError=True):
                if hasattr(dataset, "metadata"):
                    print("Dataset metadata: %s" % (dataset.metadata))
                else:
                    print("Dataset: %s" % (dataset.name))

            print("Query passed as expected (Case 5: * query)")
        except:
            assert False, "Error. All metadata keys retrieval unsuccessful."

if __name__ == '__main__':
    unittest.main()