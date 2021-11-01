#!/usr/bin/python
from datacat import client_from_config_file
from datacat.error import DcException
from datetime import datetime
import os
import sched
import subprocess
import sys
import time

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
        cksum = cksum_out[0]
        return cksum

    def get_metadata(self, dataset, dataset_location):
        """
        Extract metadata from :param path
        """
        file_path = dataset_location.resource
        # FIXME: Possibly based on value in Dataset, extract metadata
        return {}

    def run(self):
        sys.stdout = open(path+'/crawler_out.txt', 'w')
        sys.stderr = open(path+'/crawler_err.txt', 'w')
        sys.stdout.write(f"Checking for new datasets at {datetime.now().ctime()}\n")
        try:
            results = self.client.search(
                WATCH_FOLDER + "/**", version="current", site=WATCH_SITE,
                query="scanStatus = 'UNSCANNED' or scanStatus = 'MISSING'", max_num=1000
            )
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
            assert dataset_location is not None, "Error: We should never get an empty dataset location"
            file_path = dataset_location.resource
            dataset_path = dataset.path
            try:
                stat = os.stat(file_path)
                cksum = self.get_cksum(dataset_location)

                # Note: While there may only be one version of a dataset,
                # we tie the metadata to versionMetadata
                scan_result = {
                    "size": stat.st_size,
                    "checksum": str(hex(int(cksum))).lstrip("0x"),
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
