#!/usr/bin/python
from datacat import client_from_config_file
from datacat.error import DcException
from datetime import datetime
import os
import sched
import subprocess
import sys
import time
import argparse

__author__ = 'bvan'

"""
A Simple single-threaded crawler-like application.
This crawler only scans one folder at a time, retrieving up to 1000 results at a time.
It searches for datasets which are unscanned for a particular location.
"""

WATCH_FOLDER = '/CDMS'
WATCH_SITE = 'SLAC'

path = os.path.dirname(__file__)


class Crawler:
    RERUN_SECONDS = 300

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-debug_mode", action='store_true')
        self.args = parser.parse_args()
        self.client = client_from_config_file()  # Reads default config files or returns a default config
        self.sched = sched.scheduler(time.time, time.sleep)
        self._run()

    def start(self):
        self.sched.run()

    def _run(self):
        self.run()
        self.sched.enter(Crawler.RERUN_SECONDS, 1, self._run, ())

    def get_cksum(self, path):
        cksum_proc = subprocess.Popen(["cksum", path.resource], stdout=subprocess.PIPE)
        ec = cksum_proc.wait()
        if ec != 0:
            # Handle error here, or raise exception/error
            pass
        cksum_out = cksum_proc.stdout.read().split(b" ")
        # 0: checksum 1: size
        cksum = cksum_out[0].decode("UTF-8")
        ck = str(hex(int(cksum))).strip("0x")
        return ck

    def get_metadata(self, dataset, dataset_location):
        """
        Extract metadata from :param path
        """
        file_path = dataset_location.resource

        if not hasattr(dataset, "versionId"):
            assert ValueError("Dataset has no versionId")

        ds = self.client.path(path=dataset.path + ";v={}".format(dataset.versionId))
        return dict(ds.versionMetadata)

    def run(self):
        sys.stdout = open(path+'/crawler_out.txt', 'w')
        sys.stderr = open(path+'/crawler_err.txt', 'w')
        sys.stdout.write(f"Checking for new datasets at {datetime.now().ctime()}\n")
        try:
            if self.args.debug_mode:
                use_watch_folder = "testpath"
            else:
                use_watch_folder = WATCH_FOLDER
            results = self.client.search(use_watch_folder + "/**", version="current", site=WATCH_SITE,
                                         query="scanStatus = 'UNSCANNED' or scanStatus = 'MISSING'", max_num=1000)
        except DcException as error:
            sys.stderr.write(str(error))
            return False

        for dataset in results:
            locations = dataset.locations
            dataset_location = None
            for location in locations:
                if location.site == WATCH_SITE:
                    dataset_location = location
                    break
            # We should be guaran
            assert dataset_location is not None, "Error: We96 should never get an empty dataset location"
            file_path = dataset_location.resource
            dataset_path = dataset.path

            try:
                stat = os.stat(file_path)
                cksum = self.get_cksum(dataset_location)

                # Note: While there may only be one version of a dataset,
                # we tie the metadata to versionMetadata
                scan_result = {
                    "size": stat.st_size,
                    "checksum": cksum,
                    # UTC datetime in ISO format (Note: We need Z to denote UTC Time Zone)
                    "locationScanned": datetime.utcnow().isoformat()+"Z",
                    "scanStatus": "OK"
                }

                md = self.get_metadata(dataset, dataset_location)
                if md:
                    scan_result["versionMetadata"] = md
            except PermissionError as e:
                sys.stderr.write(f"Permission denied: {file_path}\n")
                sys.stderr.flush()
                continue
            except FileNotFoundError as e:
                scan_result = {
                    "scanStatus": "MISSING",
                }
                sys.stdout.write(f"Error: File {file_path} not found for {WATCH_SITE}. Status set to MISSING\n")
            try:
                patched_ds = self.client.patch_dataset(dataset_path, scan_result, versionId=dataset.versionId,
                                                       site=WATCH_SITE)
                sys.stdout.write("How we would have updated Dataset:\n")
                sys.stdout.write(str(patched_ds)+"\n")
                sys.stdout.flush()
            except DcException as error:
                sys.stderr.write(str(error)+"\n")
                sys.stderr.flush()
                continue
        sys.stdout.close()
        sys.stderr.close()
        return True


def main():
    c = Crawler()
    c.start()


if __name__ == '__main__':
    main()
