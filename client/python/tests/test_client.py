

import os
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

__author__ = 'klo'

''' This test created 3 datasets. The first two are the predecessors of the last one.
'''
if __name__ == "__main__":

    # datacat
    config_file = os.path.dirname(__file__) + '/config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    # file/datacatalog path
    file_path = '~/data'
    datacat_path = '/testpath'

    # metadata
    metadata = Metadata()
    metadata['nIsTest'] = 1

    filename = "dataset001.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file001 = file_path + '/' + filename
    ds001 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                     versionMetadata=metadata,
                     resource=full_file001,
                     site='SLAC')
    print(vars(ds001))

    filename = "dataset002.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file002 = file_path + '/' + filename
    ds002 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                        versionMetadata=metadata,
                        resource=full_file002,
                        site='SLAC')
    print(vars(ds002))

    filename = "dataset003.dat"
    if client.exists(datacat_path + '/' + filename):
        client.rmds(datacat_path + '/' + filename)
    full_file003= file_path + '/' + filename
    ds003 = client.mkds(datacat_path, filename, 'JUNIT_TEST', 'junit.test',
                    versionMetadata=metadata,
                    resource=full_file003,
                    site='SLAC')
    print(vars(ds003))

    did001 = client.get_dependency_id(ds001)
    did002 = client.get_dependency_id(ds002)
    did003 = client.get_dependency_id(ds003)
    vmetadata = { "dependency": did003,
                  "dependencyName": "test_data",
                  "dependents": [did001, did002],
                  "dependentType": "predecessor" }
    client.mkgroup(datacat_path+'/'+filename, metadata=vmetadata)
    # ToDo: Get dependents from the created group's metadata
    dependents = ""
    print(dependents)