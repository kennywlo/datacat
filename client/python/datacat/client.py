
import os
from .auth import auth_from_config
from .config import config_from_file
from .error import DcClientException, checked_error
from .http_client import HttpClient
from .model import *


# noinspection PyPep8Naming,PyShadowingBuiltins,PyUnusedLocal
class Client(object):

    """
    Pythonic Client for interacting with the data catalog. This client interacts solely through JSON.
    """

    def __init__(self, url=None, auth_strategy=None, **kwargs):
        if not url:
            raise ValueError("Client has no API URL configured")
        self.http_client = HttpClient(url, auth_strategy, **kwargs)
        self.url = url
        self.dependency_cache = {}

    @checked_error
    def path(self, path, versionId=None, site=None, stat=None):
        resp = self.http_client.path(path, versionId, site)
        return unpack(resp.content)

    @checked_error
    def children(self, path, versionId=None, site=None, stat=None, offset=None, max_num=None):
        resp = self.http_client.children(path, versionId, site, stat, offset, max_num)
        return unpack(resp.content)

    def exists(self, path, versionId=None, site=None):
        try:
            self.path(path, versionId, site)
            return True
        except DcClientException as e:
            if "NoSuchFile" in e.type:
                return False
            raise e

    @checked_error
    def mkdir(self, path, type="folder", parents=False, metadata=None, **kwargs):
        """
        Make a new Container
        :param path: Container Target path
        :param type: Container type. Defaults to folder.
        :param parents: If true, will create intermediate Folders as required.
        :param metadata: Metadata to add to when creating folder
        :param kwargs: Additional attributes to add to the container object
        :return: A :class`requests.Response` object. A user can use Response.content to get the content.
        The object will be a Folder
        """
        container = None
        if type.lower() == "folder":
            container = Folder(path=path, name=path.split("/")[-1], metadata=metadata, **kwargs)
        elif type.lower() == "group":
            container = Group(path=path, name=path.split("/")[-1], metadata=metadata, **kwargs)
        if parents:
            parts = []
            parentpath = os.path.dirname(path)
            while not self.exists(parentpath):
                parts.append(os.path.split(parentpath)[1])
                parentpath = os.path.dirname(parentpath)
            if len(parts):
                for part in reversed(parts):
                    parentpath = os.path.join(parentpath, part)
                    self.mkdir(parentpath)
        resp = self.http_client.mkdir(path, payload=pack(container), type=type)
        return unpack(resp.content)

    def mkfolder(self, path, parents=False, metadata=None, **kwargs):
        """
        Make a new Folder
        :param path: Container Target path
        :param parents: If true, will create intermediate Folders as required.
        :param metadata: Metadata to add to when creating folder
        :param kwargs: Additional attributes to add to the folder object
        """
        return self.mkdir(path, "folder", parents, metadata, **kwargs)

    def mkgroup(self, path, parents=False, metadata=None, **kwargs):
        """
        Make a new Folder
        :param path: Container Target path
        :param parents: If true, will create intermediate Folders as required.
        :param metadata: Metadata to add to when creating folder
        :param kwargs: Additional attributes to add to the folder object
        :return: A :class`requests.Response` object. A user can use Response.content to get the content.
        The object will be a Folder
        """
        return self.mkdir(path, "group", parents, metadata, **kwargs)

    # @checked_error
    # def get_dependent_id(self, datasets):
    #     """
    #     Fetch the identifiers used for dataset dependency.
    #     :param datasets: one or more datasets
    #     :return: the dependent identifiers (versionPk) of the input datasets
    #     """
    #     if datasets is None:
    #         return None
    #     if isinstance(datasets, Dataset):
    #         return datasets.versionPk
    #     ids = []
    #     for ds in datasets:
    #         if hasattr(ds, "versionPk"):
    #             ids.append(ds.versionPk)
    #     return ids

    @checked_error
    def get_dependents(self, dep_container, dep_type, max_depth, chunk_size):
        """
        Retrieves dependents up to the provided "chunk_size" at a time, subject to "max_depth".

        :param dep_container: Parent container to get dependents from.
        :param dep_type: Type of dependents to get.
        :param max_depth: Depth of dependency chain.
        :param chunk_size: Total amount of dependents retrieved.
        :return: List of retrieved dependents.
        """

        # ----------------------------------------
        # Nested Method (Start)
        # ----------------------------------------
        def retrieveContainerDependents(dep_container_processed):
            """
            Method which retrieves dependents from a provided container (is to be used recursively)
            :param dep_container_processed: Parent container object you wish to get dependents from.
            :return: List of dependents object attached to parent container
            """

            nonlocal remaining_chunk_size
            nonlocal currentDepth
            nonlocal process_queue

            innerDependents = []

            # The provided container is a Dataset
            if isinstance(dep_container_processed, Dataset):

                if currentDepth == 0:
                    dependentsToRetrieve = dep_container_processed.versionMetadata[dep_type + '.dataset']

                try:
                    if currentDepth > 0:
                        dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                except:
                    pass

                # Iterate through the provided container dependents, retrieving each dependent one at a time.
                searchResults = self.search(target=container_dependency_name,
                                            show="dependents",
                                            query='dependents in ({''})'.format(dependentsToRetrieve),
                                            ignoreShowKeyError=True)

                dependentQueue = copy.deepcopy(searchResults)

                try:
                    for i in searchResults:
                        if remaining_chunk_size <= 0:
                            # We now need to update the cache to contain information about where we stopped
                            # retrieving dependents

                            self.dependency_cache[container_dependency_name]["all_dependents_for_current_container"] = dependentsToRetrieve
                            self.dependency_cache[container_dependency_name]["dependents_left"] = dependentQueue

                            # Some important features we might want to keep stored in cache are...
                            # Dependency Name
                            # Current Depth
                            # ALL Dependents at current level
                            # Dependents that are yet to be returned.

                            # From this we can calculate the next level and still return the current dependents at
                            # the current level

                            return innerDependents
                        else:
                            # retrieve up to "chunk size" dependents
                            innerDependents.append(i)
                            try:
                                self.dependency_cache[container_dependency_name]["dependents_retrieved_so_far"].append(i)
                            except Exception as e:
                                print(e)
                            dependentQueue.pop(0)
                            remaining_chunk_size = remaining_chunk_size - 1
                except IndexError:
                    print("No more dependents to retrieve!")
                    pass

                # Return the retrieved dependents

                return innerDependents

            # The provided container is a Group
            elif isinstance(dep_container_processed, Group):
                # TODO: get dependents from the group dependency, to be cached
                return dependents

            # The provided container is neither a Dataset or a Group
            else:
                raise ValueError("Unrecognized dependency container")

        # ----------------------------------------
        # Nested Method (END)
        # ----------------------------------------

        currentDepth = 0  # Current depth should always start at 1.
        containersToProcess = []  # The dependency containers we are processing.
        newContainersToProcess = []  # The next iteration of containers to process, depending on the depth.
        dependents = []  # arraylist for returned dependents to be stored

        # If dataset, store the dependency name and its corresponding dependents
        container_dependency_name = dep_container.versionMetadata["dependencyName"]
        # Create a dictionary that will hold all of the current dependencies needed cache information
        dependency_information = {"current_depth": None,
                                  "all_containers_at_current_depth": None,
                                  "containers_left_to_process": None,
                                  "all_dependents_for_current_container": None,
                                  "dependents_retrieved_so_far": [],
                                  "dependents_left": None,
                                  "chunk_size": chunk_size,
                                  "max_depth": max_depth,
                                  "dep_type": dep_type}
        # Add the dependency to the cache, given that it doesnt already exist
        self.dependency_cache[container_dependency_name] = dependency_information

        currentObject = self.dependency_cache.get(container_dependency_name)
        remaining_chunk_size = currentObject.get("chunk_size")
        # The first iteration to process will always be the user defined one
        containersToProcess.append(dep_container)
        process_queue = containersToProcess.copy()

        dependents_already_retrieved = []

        # Iterates through each level starting at level 1 and ending at the user providing max_depth.
        for currentDepth in range(max_depth):
            # Iterate through each container stored in containersToProcess
            for x in containersToProcess:
                try:
                    try:
                        # For each container retrieve its dependents using the method "retrieveContainerDependents(x)"
                        newContainersToProcess = []
                        newContainersToProcess.extend(retrieveContainerDependents(x))
                        dependents = dependents + newContainersToProcess


                    except:
                        del process_queue[0]
                        continue

                    if remaining_chunk_size > 0:
                        del process_queue[0]

                    if remaining_chunk_size <= 0:
                        self.dependency_cache[container_dependency_name]["current_depth"] = currentDepth
                        self.dependency_cache[container_dependency_name]["all_containers_at_current_depth"] = containersToProcess
                        self.dependency_cache[container_dependency_name]["containers_left_to_process"] = process_queue

                        return dependents

                except:
                    pass

            containersToProcess.clear()
            containersToProcess = dependents_already_retrieved + newContainersToProcess
            process_queue = copy.deepcopy(containersToProcess)
            dependents_already_retrieved.clear()
            self.dependency_cache[container_dependency_name]["containers_left_to_process"] = dependents_already_retrieved
            self.dependency_cache[container_dependency_name]["dependents_retrieved_so_far"] = []


            if not containersToProcess:
                self.dependency_cache.pop(container_dependency_name)
                return dependents
            else:
                newContainersToProcess.clear()

        return dependents


    @checked_error
    def get_next_dependents(self, dep_container):
        """
         Retrieve next dependents attached to container object.
        :param dep_container: Parent container object you wish to get next dependents from
        :return: List of dependent objects attached to container object
        """

        # ----------------------------------------
        # Nested Method (Start)
        # ----------------------------------------
        def retrieveContainerDependents(dep_container_processed):
            """
            Method which retrieves dependents from a provided container (is to be used recursively)
            :param dep_container_processed: Parent container object you wish to get dependents from.
            :return: List of dependents object attached to parent container
            """

            nonlocal y
            nonlocal dependencyName
            nonlocal remaining_chunk_size
            nonlocal current_depth
            nonlocal container_queue
            nonlocal currentDCItem
            nonlocal dependents_already_retrieved

            innerDependents = []

            # The provided container is a Dataset
            if isinstance(dep_container_processed, Dataset):


                dependentsToRetrieve = str(self.get_dependent_id(currentDCItem.get("dependents_left")))
                dependentsToRetrieve = dependentsToRetrieve.replace('[', '')
                dependentsToRetrieve = dependentsToRetrieve.replace(']', '')

                if dependents_already_retrieved == [] and (dependentsToRetrieve == '' or dependentsToRetrieve ==
                                                           "None"):
                    if y == 0:
                        dependentsToRetrieve = dep_container_processed.versionMetadata[dep_type + '.dataset']
                    try:
                        if y > 0:
                            dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                    except:
                        empty = []
                        return empty

                # if dependentsToRetrieve == '':
                #    print("No more dependents!")
                #    empty = []
                #   return empty

                # Iterate through the provided container dependents, retrieving each dependent one at a time.
                searchResults = self.search(target=dependencyName,
                                            show="dependents",
                                            query='dependents in ({''})'.format(dependentsToRetrieve),
                                            ignoreShowKeyError=True)

                dependentQueue = copy.deepcopy(searchResults)

                try:
                    for i in searchResults:
                        if remaining_chunk_size <= 0:
                            # We now need to update the cache to contain information about where we stopped
                            # retrieving dependents

                            self.dependency_cache[dependencyName][
                                "all_dependents_for_current_container"] = dependentsToRetrieve
                            self.dependency_cache[dependencyName]["dependents_left"] = dependentQueue

                            # Some important features we might want to keep stored in cache are...
                            # Dependency Name
                            # Current Depth
                            # ALL Dependents at current level
                            # Dependents that are yet to be returned.

                            # From this we can calculate the next level and still return the current dependents at
                            # the current level

                            return innerDependents
                        else:
                            # retrieve up to "chunk size" dependents

                            datasetCurr = i
                            innerDependents.append(i)
                            self.dependency_cache[dependencyName]["dependents_retrieved_so_far"].append(i)
                            dependentQueue.pop(0)
                            remaining_chunk_size = remaining_chunk_size - 1
                except IndexError as e:
                    print(e)
                    pass

                # Return the retrieved dependents
                self.dependency_cache[dependencyName]["dependents_left"] = dependentQueue
                return innerDependents

            # The provided container is a Group
            elif isinstance(dep_container_processed, Group):
                # TODO: get dependents from the group dependency, to be cached
                return innerDependents

            # The provided container is neither a Dataset or a Group
            else:
                raise ValueError("Unrecognized dependency container")

        # ----------------------------------------
        # Nested Method (END)
        # ----------------------------------------

        # Now we have the information needed to continue from where we last left off.
        # That means that get_next_dependents will be extremely similar to get_dependents.
        # I just need to be able to change what parameters go into it at first

        newContainersToProcess = []

        # Step 1:
        # Well first things first, lets get all the information we need read in from the cache.

        dependencyName = dep_container.versionMetadata["dependencyName"]

        currentDCItem = self.dependency_cache.get(dependencyName)

        if currentDCItem is None:
            print("ERROR: Dependency not found in cache!")
            return

        max_depth = currentDCItem.get("max_depth")
        current_depth = currentDCItem.get("current_depth")
        remaining_chunk_size = currentDCItem.get("chunk_size")
        all_containers_at_current_depth = currentDCItem.get("all_containers_at_current_depth")
        containersToProcess = currentDCItem.get("containers_left_to_process")
        container_queue = currentDCItem.get("containers_left_to_process")
        dependents_already_retrieved = copy.deepcopy(currentDCItem.get("dependents_retrieved_so_far"))
        dep_type = currentDCItem.get("dep_type")

        if dependents_already_retrieved is None:
            dependents_already_retrieved = []

        dependentsRetrieved = []

        process_queue = copy.deepcopy(container_queue)

        for y in range(max_depth)[current_depth:]:
            for x in container_queue:

                try:

                    try:
                        newContainersToProcess.extend(retrieveContainerDependents(x))
                        dependentsRetrieved = dependentsRetrieved + newContainersToProcess
                    except:
                        del process_queue[0]
                        continue

                    if remaining_chunk_size > 0:
                        del process_queue[0]

                    if remaining_chunk_size <= 0:
                        self.dependency_cache[dependencyName]["current_depth"] = y
                        self.dependency_cache[dependencyName]["all_containers_at_current_depth"] = containersToProcess
                        self.dependency_cache[dependencyName]["containers_left_to_process"] = process_queue

                        return dependentsRetrieved
                except:
                    pass

            container_queue.clear()
            container_queue = dependents_already_retrieved + newContainersToProcess
            process_queue = copy.deepcopy(container_queue)
            dependents_already_retrieved.clear()
            self.dependency_cache[dependencyName]["containers_left_to_process"] = dependents_already_retrieved
            self.dependency_cache[dependencyName]["dependents_retrieved_so_far"] = []

        if not container_queue:
            print("Finished Processing Dependency: ", dependencyName)
            self.dependency_cache.pop(dependencyName)
            return dependentsRetrieved
        else:
            newContainersToProcess.clear()

        return dependentsRetrieved

    @checked_error
    def add_dependents(self, dep_container, dep_type, dep_datasets=None, dep_groups=None):
        """
         Attach new dependents to container object.
        :param dep_container: Parent container object to add dependents to
        :param dep_type: Type of dependents to add
        :param dep_datasets: The datasets we wish to use as children of the parent container.
        VersionPKs are required for each dependent dataset.
        :param dep_groups: The groups we wish to use as children of the parent container
        """

        def get_dependent_id(datasets):
            """
            Fetch the identifiers used for dataset dependency.
            :param datasets: one or more datasets
            :return: the dependent identifiers (versionPk) of the input datasets
            """
            if datasets is None:
                return None
            if isinstance(datasets, Dataset):
                return datasets.versionPk
            ids = []
            for ds in datasets:
                if hasattr(ds, "versionPk"):
                    ids.append(ds.versionPk)
                else:
                    raise ValueError("Could not retrieve dependent dataset versionPK.")
            return ids

        def convert_dependent_to_list(dependents):
            """
            :param dependents: string of comma seperated dependents
            :return: list of dependents int
            """
            dependent_list = list(dependents.split(","))
            int_list = []
            # convert dependent to int
            for dependent in dependent_list:
                int_list.append(int(dependent))
            return int_list

        if isinstance(dep_container, Dataset):
            container = Dataset(**dep_container.__dict__)
            if dep_datasets is not None and all(isinstance(x, Dataset) for x in dep_datasets):

                # default configuration
                dependents = get_dependent_id(dep_datasets)
                dependency_metadata = {"dependents": str(dependents),
                                        "dependentType": dep_type}

                if hasattr(container, "versionMetadata"):
                    # if container has versionMetadata field
                    # Get dependency metadata from container
                    vmd = dict(container.versionMetadata)

                    if "{}.dataset".format(dep_type) in vmd.keys():
                        # if adding dependents of the same dependent type:
                        # merge dependents of same type
                        update_dependentType = dep_type
                        update_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))
                        dependents = list(set(dependents + update_dependents))
                        dependency_metadata["dependents"] = str(dependents)

                    container.versionMetadata.update(dependency_metadata)
                else:
                    # if container doesn't have metadata field
                    # provide dependency metadata as new metadata field
                    container.versionMetadata = dependency_metadata

                # patching with patchds()
                try:
                    returned = self.patchds(path=dep_container.path, dataset=container, versionId=dep_container.versionId)
                except:
                    assert False, "Failed to add dependents"


            else:
                raise ValueError("Unrecognized dependent dataset object")

            return True

        elif isinstance(dep_container, Group):

            return True
        else:
            raise ValueError("Unrecognized dependency container")


    @checked_error
    def remove_dependents(self, dep_container, dep_type, dep_datasets=None, dep_groups=None):
        """
        Remove dependents from container object provided
        :param dep_container: Parent container object to remove dependents from
        :param dep_type: Type of dependents to remove
        :param dep_datasets: The datasets we wish to remove from the parent container
        :param dep_groups: The groups we wish to remove from the parent container
        """

        def get_dependent_id(datasets):
            """
            Fetch the identifiers used for dataset dependency.
            :param datasets: one or more datasets
            :return: the dependent identifiers (versionPk) of the input datasets
            """
            if datasets is None:
                return None
            if isinstance(datasets, Dataset):
                return datasets.versionPk
            ids = []
            for ds in datasets:
                if hasattr(ds, "versionPk"):
                    ids.append(ds.versionPk)
                else:
                    raise ValueError("Could not retrieve dependent dataset versionPK.")
            return ids

        def convert_dependent_to_list(dependents):
            """
            convert string of comma seperated versionPk of dependent datasets to list of int.
            :param dependents: string of comma seperated dependents
            :return: list of dependents int
            """
            dependent_list = list(dependents.split(","))
            int_list = []
            # convert dependent to int
            for dependent in dependent_list:
                int_list.append(int(dependent))
            return int_list

        def check_exist(update_dp, remove_dp):
            """
            To ensure user provided dependent list is in container dependents.
            :param update_dp: list of dependent versionPk (int) of container dataset
            :param remove_dp: list of dependent versionPk (int) of user provided dep_datasets

            """
            remove_set = set(remove_dp)
            update_set = set(update_dp)
            if not remove_set.issubset(update_set):
                raise ValueError("Dependent does not exsit.")

        if isinstance(dep_container, Dataset):
            container = Dataset(**dep_container.__dict__)

            # Ensure dep_datasets presents and is dataset object
            if dep_datasets is not None and all(isinstance(x, Dataset) for x in dep_datasets):

                # Ensure container dataset has versionMetadata and versionPk field
                if all(hasattr(container, attr) for attr in ["versionMetadata", "versionPk"]):
                    vmd = dict(container.versionMetadata)
                    remove_dependents = get_dependent_id(dep_datasets)

                    # Ensure dependent type provided by user is present in container dataset
                    if "{}.dataset".format(dep_type) in vmd.keys():
                        update_dependentType = dep_type
                        update_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))
                    else:
                        raise ValueError("No dependents of type {} found".format(dep_type))

                    # Ensure user provided dependents are in container
                    check_exist(update_dependents, remove_dependents)

                    # Step 1: remove dependents provided by users from container

                    for dependent in remove_dependents:
                        update_dependents.remove(dependent)

                    # Step 2: update dependency metadata with new dependents

                    # if user removes all dependents
                    # construct empty string dependency metadata
                    if len(update_dependents) == 0:
                        update_dependents = ""
                        update_dependentType = ""

                    container.versionMetadata = {"dependents": str(update_dependents),
                                                "dependentType": update_dependentType}

                    # use patchds() to patch dataset container
                    try:
                        returned = self.patchds(path=dep_container.path, dataset=container, versionId=dep_container.versionId)
                    except:
                        assert False, "Failed to remove dependents"
                else:
                    raise ValueError("Container doesn't have versionMetadata.")
            else:
                raise ValueError("Unrecognized dependent dataset object")
            return True

        elif isinstance(dep_container, Group):
            # TODO: remove dependents from the group dependency
            return True
        else:
            raise ValueError("Unrecognized dependency container")

    @checked_error
    def mkds(self, path, name, dataType, fileFormat, versionId="new", site=None, resource=None, versionMetadata=None,
             **kwargs):
        """
        Make a dataset.
        :param path: Container Target path
        :param name: Name of Dataset you wish to create
        :param dataType: User-Defined Data Type of Dataset. This is often a subtype of a file format.
        :param fileFormat: The File Format of the Dataset (i.e. root, fits, tar.gz, txt, etc...)
        :param versionId: Desired versionId. By default, it is set to "new", which will result in a versionId of 0.
        :param site: Site where the dataset physically resides (i.e. SLAC, IN2P3)
        :param versionMetadata: Metadata to add to registered version if registering a version.
        :param resource: The actual file resource path at the given site (i.e. /nfs/farm/g/glast/dataset.dat)
        :param kwargs: Additional attributes to pass through to build_dataset
        :return: A representation of the dataset that was just created.
        """
        ds = build_dataset(name, dataType, fileFormat, site=site, versionId=versionId, resource=resource,
                           versionMetadata=versionMetadata, **kwargs)
        resp = self.http_client.mkds(path, pack(ds), **kwargs)
        return unpack(resp.content)

    # noinspection PyIncorrectDocstring
    def create_dataset(self, path, name, dataType, fileFormat,
                       versionId="new", site=None, versionMetadata=None, resource=None, **kwargs):
        """
        See mkds
        """
        return self.mkds(path, name, dataType, fileFormat, versionId, site, resource, versionMetadata, **kwargs)

    @checked_error
    def mkver(self, path, versionId="new", site=None, resource=None, versionMetadata=None, **kwargs):
        """
        Make a dataset version and optionally include enough information for a location
        :param path: Target Dataset path
        :param versionId: Desired versionId. By default, it is set to "new", which will result in the next version id.
        :param site: Site where the dataset physically resides (i.e. SLAC, IN2P3)
        :param versionMetadata: Metadata to add
        :param resource: The actual file resource path at the given site (i.e. /nfs/farm/g/glast/dataset.dat)
        :param kwargs: Additional version or location attributes
        :return: A representation of the dataset that was just created.
        """
        # We piggy back off of build_dataset
        ds = build_dataset(versionId=versionId, site=site, resource=resource, versionMetadata=versionMetadata)
        resp = self.http_client.mkds(path, pack(ds), **kwargs)
        return unpack(resp.content)

    @checked_error
    def mkloc(self, path, site, resource, versionId="current", **kwargs):
        """
        Make a dataset location.
        :param path: Target Dataset path
        :param site: Site where the dataset physically resides (i.e. SLAC, IN2P3)
        :param versionId: Desired versionId to add this dataset to. Defaults to current version.
        :param resource: The actual file resource path at the given site (i.e. /nfs/farm/g/glast/dataset.dat)
        :param kwargs: Additional location attributes
        :return: A representation of the dataset that was just created.
        """
        location = dict(site=site, resource=resource)
        location.update(**kwargs)
        ds = build_dataset(location=location)
        resp = self.http_client.mkds(path, pack(ds), versionId=versionId)
        return unpack(resp.content)

    @checked_error
    def rmdir(self, path, type="folder", **kwargs):
        """
        Remove a container.
        :param path: Path of container to remove.
        :param type: Type of container (Group or Folder). This will be removed in a future version.
        :return: A :class`requests.Response` object. A client can inspect the status code.
        """
        self.http_client.rmdir(path, type)
        return True

    @checked_error
    def rmds(self, path, **kwargs):
        """
        Remove a dataset.
        :param path: Path of dataset to remove
        :param kwargs:
        """
        self.http_client.rmds(path)
        return True

    # noinspection PyIncorrectDocstring
    def delete_dataset(self, path, **kwargs):
        """
        See rmds
        """
        return self.rmds(path)

    @checked_error
    def patchdir(self, path, container, type="folder", **kwargs):
        """
        Patch a container.
        :param path: Path of the dataset to patch.
        :param type: Container type. Defaults to folder.
        :param container: A dict object or a dataset.model.Group/Folder object representing the changes to be applied
        to the container.
        :param kwargs:
        :return: A representation of the patched dataset
        """
        if isinstance(container, Container):
            pass
        elif type == "group":
            container = Group(**container)
        elif type == "folder":
            container = Folder(**container)

        resp = self.http_client.patchdir(path, pack(container), type, **kwargs)
        return unpack(resp.content)

    @checked_error
    def patchds(self, path, dataset, versionId="current", site=None):
        """
        Patch a dataset.
        :param path: Path of the dataset to patch.
        :param dataset: A dict object or a dataset.model.Dataset object representing the changes to be applied to the
        dataset
        :param versionId: If specified, identifies the version to patch. Otherwise, it's assumed to patch the current
        version, should it exist.
        :param site: If specified, identifies the specific location to be patched (i.e. SLAC, IN2P3)
        :return: A representation of the patched dataset
        """
        ds = dataset if type(dataset) == Dataset else Dataset(**dataset)
        resp = self.http_client.patchds(path, pack(ds), versionId, site)
        return unpack(resp.content)

    # noinspection PyIncorrectDocstring
    def patch_container(self, path, container, type="folder", **kwargs):
        """
        See patchdir.
        """
        return self.patchdir(path, container, type, **kwargs)

    # noinspection PyIncorrectDocstring
    def patch_dataset(self, path, dataset, versionId="current", site=None, **kwargs):
        """
        See patchds
        """
        return self.patchds(path, dataset, versionId, site, **kwargs)

    @checked_error
    def search(self, target, versionId=None, site=None, query=None, sort=None, show=None, offset=None, max_num=None,
               **kwargs):
        """Search a target. A target is a Container of some sort. It may also be specified as a glob, as in:
         1. /path/to - target /path/to _only_
         2. /path/to/* - target is all containers directly in /path/to/
         3. /path/to/** - target is all containers, recursively, under /path/to/
         4. /path/to/*$ - target is only folders directly under /path/to/
         5. /path/to/**^ - target is only groups, recursively, under /path/to/

        :param target: The path (or glob-like path) of which to search
        :param versionId: Optional VersionId to filter by
        :param site: Optional site to filter by
        :param query: The query
        :param sort: Fields and Metadata fields to sort on.
        :param show: Metadata fields to optionally return
        :param offset: Offset at which to start returning objects.
        :param max_num: Maximum number of objects to return.
        """
        resp = self.http_client.search(target, versionId, site, query, sort, show, offset, max_num, **kwargs)
        return unpack(resp.content)

    @checked_error
    def permissions(self, path, group=None):
        """
        Retrieve the effective permissions.
        :param path: Path of the object to retrieve.
        :param group: If specified, this should be a group specification of the format "{name}@{project}"
        :return: A :class`datacat.model.AclEntry` object representing permissions
        """
        resp = self.http_client.permissions(path, group)
        return unpack(resp.content)

    @checked_error
    def listacl(self, path):
        """
        Retrieve the Access Control List of a given path
        :param path: Path of the object to retrieve.
        :return: A list of :class`datacat.model.AclEntry` objects.
        """
        resp = self.http_client.listacl(path)
        return unpack(resp.content)

    @checked_error
    def patchacl(self, path, acl):
        """
        Patch an ACL.
        :param path: Path of the object to patch (must be container)
        :param acl: A list of :class`datacat.model.AclEntry` objects or a similar dict
        :return: An updated list of :class`datacat.model.AclEntry` objects.
        """
        resp = self.http_client.patchacl(path, pack(acl))
        return unpack(resp.content)


def client_from_config_file(path=None, override_section=None):
    """
    Return a new client from a config file.
    :param path: Path to read file from. If None, will read from
     default locations.
    :param override_section: Section in config file with overridden
     values. If None, only defaults section will be read.
    :return: Configured client
    :except: OSError if path is provided and the file doesn't exist.
    """
    config = config_from_file(path, override_section)
    return client_from_config(config)


def client_from_config(config):
    auth_strategy = auth_from_config(config)
    return Client(auth_strategy=auth_strategy, **config)
