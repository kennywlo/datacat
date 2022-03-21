import argparse
import os,sys
import datacat
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":

    # datacat
    config_file = './config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)


    # file/datacatalog path
    file_name = 'test_dc0.6_1.root'
    file_path = os.path.abspath("../../../test/data/")
    datacat_path = '/testpath/testfolder'
    full_file = file_path + '/' + file_name

    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    if client.exists(datacat_path+'/' + file_name):
        client.rmds(datacat_path+ '/' + file_name)

    ds = client.mkds(datacat_path, file_name, 'JUNIT_TEST','junit.test',
                     versionMetadata=metadata,
                     resource=full_file,
                     site='SLAC')
    print(vars(ds))

    file_name2 = 'test_dc0.6_2.root'
    full_file2 = file_path + '/' + file_name2

    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    dependents = client.client_helper.get_dependent_id(ds)
    dep_metadata = {"dependents": str(dependents),
                    "dependentType": "successor"}
    metadata.update(dep_metadata)

    if client.exists(datacat_path+'/' + file_name2):
        client.rmds(datacat_path+ '/' + file_name2)
    ds2 = client.mkds(datacat_path, file_name2, 'JUNIT_TEST', 'junit.test',
                      versionMetadata=metadata,
                      resource=full_file2,
                      site='SLAC')
    print(vars(ds2))