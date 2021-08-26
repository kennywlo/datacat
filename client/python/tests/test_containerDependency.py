import os, sys
from datacat import client_from_config, config_from_file
from datacat.model import Metadata

if __name__ == "__main__":
    # datacat
    config_file ='./config_srs.ini'
    config = config_from_file(config_file)
    client = client_from_config(config)

    containerPath1 = "/testpath/depGroup1"
    containerPath2 = "/testpath/depGroup2"

    try:
        if client.exists(containerPath1):
            client.rmdir(containerPath1, type="group")

        if client.exists(containerPath2):
            client.rmdir(containerPath2, type="group")

    except:
        print("exception caught here")

    client.mkgroup(containerPath1)
    client.mkgroup(containerPath2)