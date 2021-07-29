import argparse
import os, sys
import datacat
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":

    # datacat
    config_file = './config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_name = 'test_slac.root'
    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testfolder'
    full_file = file_path + '/' + file_name

    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    if client.exists(datacat_path + '/' + file_name):
        client.rmds(datacat_path + '/' + file_name)

    ds = client.mkds(datacat_path, file_name, 'JUNIT_TEST', 'junit.test',
                     versionMetadata=metadata,
                     resource=full_file,
                     site='SLAC')
    print(vars(ds))