import os
import copy
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

    @checked_error
    def get_dependent_id(self, dependents):
        """
        Fetch the identifiers used for dataset dependency.
        :param dependents: one or more dependents, of Dataset or Group
        :return: the identifiers of the input dependents
        """
        ids = []
        if not dependents:
            return None
        if isinstance(dependents, Dataset):
            return dependents.versionPk
        elif isinstance(dependents, Group):
            return dependents.pk
        for dep in dependents:
            if isinstance(dep, Dataset):
                if hasattr(dep, "versionPk"):
                    ids.append(dep.versionPk)
                else:
                    raise ValueError("Could not retrieve dependent dataset versionPK.")
            elif isinstance(dep, Group):
                if hasattr(dep, "pk"):
                    ids.append(dep.pk)
            else:
                raise ValueError("Unrecognized dependent type")
        return ids

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

        def encodeForCache(ToEncode):

            if isinstance(ToEncode, str):
                versionPK_list = ToEncode.split(",")
                map_list = map(int, versionPK_list)
                list_of_versionPKs = list(map_list)
                return list_of_versionPKs

            containerVersionPk = self.get_dependent_id(ToEncode)

            if isinstance(containerVersionPk, list):
                return containerVersionPk
            else:
                listConversion = []
                listConversion.append(containerVersionPk)
                return listConversion


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

            # *** "CC_Dependents" stands for "Current Container Dependents" ***
            CC_Dependents = []
            CC_Dependents_Group = []

            # The provided container is a Dataset
            # if isinstance(dep_container_processed, Dataset):
            if currentDepth == 0:
                no_response = True

                if isinstance(dep_container_processed, Dataset):
                    try:
                        dependentsToRetrieve = dep_container_processed.versionMetadata[dep_type + '.dataset']
                        no_response = False
                    except:
                        pass

                    try:
                        dependentsToRetrieve_Group = dep_container_processed.versionMetadata[dep_type + '.group']
                        no_response = False
                    except:
                        pass

                    if no_response == True:
                        return

                elif isinstance(dep_container_processed, Group):
                    try:
                        dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                        no_response = False
                    except:
                        pass

                    try:
                        dependentsToRetrieve_Group = dep_container_processed.metadata.dct[dep_type + '.group']
                        no_response = False
                    except:
                        pass

                    if no_response == True:
                        return

                # The provided container is neither a Dataset or a Group
                else:
                    raise ValueError("Unrecognized dependency container")

            try:
                if currentDepth > 0:
                    no_response = True
                    if isinstance(dep_container_processed, Dataset):
                        try:
                            dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                            no_response = False
                        except:
                            pass

                        try:
                            dependentsToRetrieve_Group = dep_container_processed.metadata.dct[dep_type + '.group']
                            no_response = False
                        except:
                            pass

                        if no_response == True:
                            return

                    elif isinstance(dep_container_processed, Group):
                        try:
                            dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                            no_response = False
                        except:
                            pass

                        try:
                            dependentsToRetrieve_Group = dep_container_processed.metadata.dct[dep_type + '.group']
                            no_response = False
                        except:
                            pass

                        if no_response == True:
                            return

                    # The provided container is neither a Dataset or a Group
                    else:
                        raise ValueError("Unrecognized dependency container")
            except:
                pass

            # Iterate through the provided container dependents, retrieving each dependent one at a time.
            no_response = True

            # Searching for all dataset dependents
            try:
                searchResults = self.search(target=dependencyName,
                                            show="dependents",
                                            query='dependents in ({''})'.format(dependentsToRetrieve),
                                            ignoreShowKeyError=True)
                no_response = False
            except:
                searchResults = []
                pass

            # Searching for all GROUP dependents
            try:
                searchResults_Groups = self.search(target=dependencyName,
                                              show="dependency.groups",
                                              containerFilter='dependentGroups in ({})'.format(dependentsToRetrieve_Group),
                                              ignoreShowKeyError=True)
                no_response = False
            except:
                searchResults_Groups = []
                pass

            if no_response == True:
                return

            # Creating 2 separate queues, one for dataset and one for GROUP
            dependentQueue = copy.deepcopy(searchResults)
            dependentQueue_Group = copy.deepcopy(searchResults_Groups)

            # For all the search results (DATASET), process them
            try:
                for dependent in searchResults:

                    # We have reached the chunk size limit, return dependents for this container
                    if remaining_chunk_size <= 0:

                        dependentsToRetrieve_encoded = encodeForCache(dependentsToRetrieve)
                        self.dependency_cache[dependencyName][
                            "all_dependents_for_current_container_datasets"] = dependentsToRetrieve_encoded

                        # ********** Convert to versionPK list - dependents_left_datasets **********
                        dependentQueue_VPK = encodeForCache(dependentQueue)
                        self.dependency_cache[dependencyName]["dependents_left_datasets"].extend(dependentQueue_VPK)
                        # self.dependency_cache[dependencyName]["dependents_left_datasets"] = dependentQueue

                        return CC_Dependents, CC_Dependents_Group

                    # We have available chunks:
                    #   Append to list of retrieved container dependents
                    #   Append to cache
                    #   Remove dependent from dependentQueue
                    else:
                        CC_Dependents.append(dependent)

                        # ********** Convert to versionPK list - dependents_retrieved_so_far_datasets **********
                        dependent_VPK = encodeForCache(dependent)
                        self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"].extend(dependent_VPK)
                        # self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"].append(dependent)

                        dependentQueue.pop(0)
                        remaining_chunk_size = remaining_chunk_size - 1
            except:
                pass

            # For all the search results (DATASET), process them
            try:
                for dependent in searchResults_Groups:

                    # We have reached the chunk size limit, return dependents for this container
                    if remaining_chunk_size <= 0:

                        dependentsToRetrieve_encoded = encodeForCache(dependentsToRetrieve_Group)
                        self.dependency_cache[dependencyName][
                            "all_dependents_for_current_container_group"] = dependentsToRetrieve_encoded

                        # ********** Convert to versionPK list - dependents_left_datasets **********
                        dependentQueue_VPK = encodeForCache(dependentQueue_Group)
                        self.dependency_cache[dependencyName]["dependents_left_group"].extend(dependentQueue_VPK)
                        # self.dependency_cache[dependencyName]["dependents_left_datasets"] = dependentQueue

                        return CC_Dependents, CC_Dependents_Group

                    # We have available chunks:
                    #   Append to list of retrieved container dependents
                    #   Append to cache
                    #   Remove dependent from dependentQueue
                    else:
                        CC_Dependents_Group.append(dependent)

                        # ********** Convert to versionPK list - dependents_retrieved_so_far_groups **********
                        dependent_VPK = encodeForCache(dependent)
                        self.dependency_cache[dependencyName]["dependents_retrieved_so_far_groups"].extend(dependent_VPK)
                        # self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"].append(dependent)

                        dependentQueue_Group.pop(0)
                        remaining_chunk_size = remaining_chunk_size - 1
            except:
                pass

            # Return the retrieved dependents
            return CC_Dependents, CC_Dependents_Group

        # ----------------------------------------
        # Nested Method (END)
        # ----------------------------------------

        # Retrieve and store the dependencyName of current container
        try:
            dependencyName = dep_container.versionMetadata["dependencyName"]
        except Exception as e:
            print("Provided container has no dependency information associated")
            return

        # Variable declaration
        currentDepth = 0  # Current depth should always start at 1.
        containersToProcess = []  # The containers we will be retrieving dependencies from.
        containersToProcess_Dataset = []    # Containers we still need to process - Datasets
        containersToProcess_Group = []  # Containers we still need to process - Groups

        nextContainersToProcess = []  # The next level of containers to process, both DS and GROUPS.
        retrieved_dependents = []  # arraylist for returned dependents to be stored

        # Will keep track of all the DATASET dependents already retrieved
        dependents_already_retrieved = []
        # Will keep track of all the GROUP dependents already retrieved
        dependents_already_retrieved_group = []

        # Create a dictionary that will create a new cache entry
        dependency_information = {"current_depth": None,

                                  "all_containers_at_current_depth_datasets": [],
                                  "all_containers_at_current_depth_groups": [],

                                  "containers_left_to_process_datasets": [],
                                  "containers_left_to_process_groups": [],

                                  "all_dependents_for_current_container_datasets": [],
                                  "all_dependents_for_current_container_groups": [],

                                  "dependents_retrieved_so_far_datasets": [],
                                  "dependents_retrieved_so_far_groups": [],

                                  "dependents_left_datasets": [],
                                  "dependents_left_groups": [],

                                  "chunk_size": chunk_size,
                                  "max_depth": max_depth,
                                  "dep_type": dep_type}

        # Add the dependency to the cache, given that it does not already exist
        # If it does exist, return
        if dependencyName in self.dependency_cache.keys():
            print("Error in method call: .get_dependents")
            print("Search process already started, please use get_next_dependency method call to continue retrieval")
            return
        else:
            self.dependency_cache[dependencyName] = dependency_information

        # Save the entire dependency object to further process
        currentDependency = self.dependency_cache.get(dependencyName)
        remaining_chunk_size = currentDependency.get("chunk_size")

        # Create a list for the two types of containers to be processed - Dataset and Group
        if isinstance(dep_container, Dataset):
            containersToProcess_Dataset.append(dep_container)
        if isinstance(dep_container, Group):
            containersToProcess_Group.append(dep_container)

        # Create a combined list of the two types - Dataset and Group
        containersToProcess.append(dep_container)
        # Create an unreferenced copy of the created list
        process_queue = copy.deepcopy(containersToProcess)

        # Iterates through each level starting at level 1 ( index 0 ) and ending at the user providing max_depth.
        for currentDepth in range(max_depth):
            # Retrieve dependents from each of the containers in containersToProcess
            for container in containersToProcess:
                try:

                    # For each container retrieve its dependents
                    try:

                        nextContainersToProcess = []
                        # returns 2 values, datasets retrieved and groups retrieved
                        next_container, next_container_Group = retrieveContainerDependents(container)

                        nextContainersToProcess.extend(next_container + next_container_Group)
                        retrieved_dependents = retrieved_dependents + nextContainersToProcess
                    except Exception as e:
                        del process_queue[0]
                        continue

                    # if we have not exceeded the chunk limit
                    if remaining_chunk_size > 0:
                        # Remove current container from queue
                        del process_queue[0]

                        # Remove current container from individual queues
                        if isinstance(container, Dataset):
                            containersToProcess_Dataset.remove(container)
                        if isinstance(container, Group):
                            containersToProcess_Group.remove(container)

                    # if we have exceeded the chunk limit
                    if remaining_chunk_size <= 0:

                        self.dependency_cache[dependencyName]["current_depth"] = currentDepth

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        containersToProcess_VPK = encodeForCache(containersToProcess_Dataset)
                        self.dependency_cache[dependencyName]["all_containers_at_current_depth_datasets"].extend(
                            containersToProcess_VPK)
                        containersToProcess_VPK = encodeForCache(containersToProcess_Group)
                        self.dependency_cache[dependencyName]["all_containers_at_current_depth_groups"].extend(
                            containersToProcess_VPK)

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        process_queue_VPK = encodeForCache(containersToProcess_Dataset)
                        self.dependency_cache[dependencyName]["containers_left_to_process_datasets"].extend(process_queue_VPK)
                        process_queue_VPK = encodeForCache(containersToProcess_Group)
                        self.dependency_cache[dependencyName]["containers_left_to_process_groups"].extend(process_queue_VPK)

                        return retrieved_dependents
                except:
                    pass

            # Prepare to process next level of containers
            containersToProcess.clear()
            containersToProcess_Dataset.clear()
            containersToProcess_Group.clear()

            # Set next level of containers to process
            # Make a freely mutable copy of that list
            containersToProcess = dependents_already_retrieved + nextContainersToProcess
            containersToProcess_Dataset = dependents_already_retrieved + next_container
            containersToProcess_Group = dependents_already_retrieved + next_container_Group
            process_queue = containersToProcess

            # Clear current level of dependents already retrieved
            # Update the cache to reflect that
            dependents_already_retrieved.clear()
            dependents_already_retrieved_group.clear()

            self.dependency_cache[dependencyName]["containers_left_to_process_datasets"] = []
            self.dependency_cache[dependencyName]["containers_left_to_process_groups"] = []

            self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"] = []
            self.dependency_cache[dependencyName]["dependents_retrieved_so_far_groups"] = []

            if not containersToProcess or currentDepth == (max_depth-1):
                print("Finished Processing the following dependency --> ", dependencyName,
                      "\nReturning last batch of dependents, if any... ")
                self.dependency_cache.pop(dependencyName)
                return retrieved_dependents
            else:
                nextContainersToProcess.clear()

        return retrieved_dependents

    @checked_error
    def get_next_dependents(self, dep_container):
        """
         Retrieve next dependents attached to container object.
        :param dep_container: Parent container object you wish to get next dependents from
        :return: List of dependent objects attached to container object
        """

        def encodeForCache(ToEncode):
            containerVersionPk = self.get_dependent_id(ToEncode)

            if isinstance(containerVersionPk, list):
                return containerVersionPk
            else:
                listConversion = []
                listConversion.append(containerVersionPk)
                return listConversion

        def decodeFromCache(ToDecode):

            if not ToDecode:
                return []

            try:
                stringConversion = [str(element) for element in ToDecode]
                ToDecodeCommaDelimited = ",".join(stringConversion)
            except Exception as e:
                print(e)

            decodeResults = self.search(target=dependencyName,
                                            show="dependents",
                                            query='dependents in ({''})'.format(ToDecodeCommaDelimited),
                                            ignoreShowKeyError=True)

            return decodeResults

        # ----------------------------------------
        # Nested Method (Start)
        # ----------------------------------------
        def retrieveContainerDependents(dep_container_processed):
            """
            Method which retrieves dependents from a provided container (is to be used recursively)
            :param dep_container_processed: Parent container object you wish to get dependents from.
            :return: List of dependents object attached to parent container
            """

            nonlocal currentDepth
            nonlocal dependencyName
            nonlocal remaining_chunk_size
            nonlocal currentDependency
            nonlocal dependents_already_retrieved

            CC_Dependents = []

            # The provided container is a Dataset
            if isinstance(dep_container_processed, Dataset):

                # Decode
                dependentsToRetrieveConversion = decodeFromCache(currentDependency.get("dependents_left_datasets"))
                dependentsToRetrieve = self.get_dependent_id(dependentsToRetrieveConversion)
                dependentsToRetrieve = str(dependentsToRetrieve)
                dependentsToRetrieve = dependentsToRetrieve.replace('[', '')
                dependentsToRetrieve = dependentsToRetrieve.replace(']', '')

                if dependents_already_retrieved == [] and (dependentsToRetrieve == '' or dependentsToRetrieve ==
                                                           "None"):
                    if currentDepth == 0:
                        dependentsToRetrieve = dep_container_processed.versionMetadata[dep_type + '.dataset']
                    try:
                        if currentDepth > 0:
                            dependentsToRetrieve = dep_container_processed.metadata.dct[dep_type + '.dataset']
                    except:
                        empty = []
                        return empty

                # Iterate through the provided container dependents, retrieving each dependent one at a time.
                searchResults = self.search(target=dependencyName,
                                            show="dependents",
                                            query='dependents in ({''})'.format(dependentsToRetrieve),
                                            ignoreShowKeyError=True)

                dependentQueue = copy.deepcopy(searchResults)

                try:
                    for dependent in searchResults:

                        # We have reached the chunk size limit, return dependents for this container
                        if remaining_chunk_size <= 0:
                            self.dependency_cache[dependencyName][
                                "all_dependents_for_current_container_datasets"] = dependentsToRetrieve

                            # ********** Convert to versionPK list - dependents_left_datasets **********
                            dependentQueue_VPK = encodeForCache(dependentQueue)
                            self.dependency_cache[dependencyName]["dependents_left_datasets"].extend(dependentQueue_VPK)
                            #   self.dependency_cache[dependencyName]["dependents_left_datasets"] = dependentQueue

                            return CC_Dependents

                        # We have available chunks:
                        #   Append to list of retrieved container dependents
                        #   Append to cache
                        #   Remove dependent from dependentQueue
                        else:
                            CC_Dependents.append(dependent)

                            # ********** Convert to versionPK list - dependents_retrieved_so_far_datasets **********
                            dependent_VPK = encodeForCache(dependent)
                            self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"].extend(dependent_VPK)
                            #   self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"].append(dependent)

                            dependentQueue.pop(0)
                            remaining_chunk_size = remaining_chunk_size - 1
                except:
                    pass

                # Return the retrieved dependents
                self.dependency_cache[dependencyName]["dependents_left_datasets"] = dependentQueue
                return CC_Dependents

            # The provided container is a Group
            elif isinstance(dep_container_processed, Group):
                # TODO: get dependents from the group dependency, to be cached
                return CC_Dependents

            # The provided container is neither a Dataset or a Group
            else:
                raise ValueError("Unrecognized dependency container")

        # ----------------------------------------
        # Nested Method (END)
        # ----------------------------------------

        try:
            dependencyName = dep_container.versionMetadata["dependencyName"]
        except Exception as e:
            print("Provided container has no dependency information associated")
            return

        currentDependency = self.dependency_cache.get(dependencyName)
        nextContainersToProcess = []
        dependents = []

        if currentDependency is None:
            print("Dependency (" + dependencyName + "): No  dependents for that container!")
            emptyReturn = []
            return emptyReturn

        max_depth = currentDependency.get("max_depth")
        current_depth = currentDependency.get("current_depth")
        remaining_chunk_size = currentDependency.get("chunk_size")

        # Decode
        all_containers_at_current_depth = decodeFromCache(currentDependency.get("all_containers_at_current_depth_datasets"))

        # Decode
        containersToProcess = decodeFromCache(currentDependency.get("containers_left_to_process_datasets"))

        # Decode
        dependents_already_retrieved_conversion = decodeFromCache(currentDependency.get("dependents_retrieved_so_far_datasets"))
        dependents_already_retrieved = copy.deepcopy(dependents_already_retrieved_conversion)
        dep_type = currentDependency.get("dep_type")

        if dependents_already_retrieved is None:
            dependents_already_retrieved = []

        process_queue = copy.deepcopy(containersToProcess)

        # Iterates through each level, resuming according to cache, and ending at the user providing max_depth.
        for currentDepth in range(max_depth)[current_depth:]:
            # Iterate through each container stored in containersToProcess
            for container in containersToProcess:
                try:
                    try:

                        # For each container retrieve its dependents
                        nextContainersToProcess = []
                        nextContainersToProcess.extend(retrieveContainerDependents(container))
                        dependents = dependents + nextContainersToProcess

                    except:
                        del process_queue[0]
                        continue

                    if remaining_chunk_size > 0:
                        del process_queue[0]

                    if remaining_chunk_size <= 0:
                        self.dependency_cache[dependencyName]["current_depth"] = currentDepth

                        # ********** Convert to versionPK list - all_containers_at_current_depth **********
                        containersToProcess_VPK = encodeForCache(containersToProcess)
                        self.dependency_cache[dependencyName]["all_containers_at_current_depth_datasets"].extend(
                            containersToProcess_VPK)
                        #   self.dependency_cache[dependencyName]["all_containers_at_current_depth"] = containersToProcess

                        # ********** Convert to versionPK list - containers_left_to_process_datasets **********
                        process_queue_VPK = encodeForCache(process_queue)
                        self.dependency_cache[dependencyName]["containers_left_to_process_datasets"].extend(process_queue_VPK)
                        #   self.dependency_cache[dependencyName]["containers_left_to_process_datasets"] = process_queue

                        return dependents
                except:
                    pass

            # Prepare to process next level of containers
            containersToProcess.clear()

            # Set next level of containers to process
            # Make a freely mutable copy of that list
            containersToProcess = dependents_already_retrieved + nextContainersToProcess
            process_queue = copy.deepcopy(containersToProcess)

            # Clear current level of dependents already retrieved
            # Update the cache to reflect that
            dependents_already_retrieved.clear()
            self.dependency_cache[dependencyName]["containers_left_to_process_datasets"] = []
            self.dependency_cache[dependencyName]["dependents_retrieved_so_far_datasets"] = []

        if not containersToProcess or currentDepth == (max_depth-1):
            print("Finished Processing the following dependency --> ", dependencyName,
                  "\nReturning last batch of dependents, if any... ")
            self.dependency_cache.pop(dependencyName)
            return dependents
        else:
            nextContainersToProcess.clear()

        return dependents

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

        def convert_dependent_to_list(dependents):
            """
            :param dependents: string of comma-delimited dependents
            :return: list of dependents int
            """
            dependent_list = list(dependents.split(","))
            int_list = []
            # convert dependent to int
            for dependent in dependent_list:
                int_list.append(int(dependent))
            return int_list

        def dependent_check(datasets, groups):

            if datasets is None and groups is None:
                assert False, "No dependents found."
            elif datasets is not None and not all(isinstance(dataset, Dataset) for dataset in dep_datasets):
                assert False, "Unrecognized dataset dependent object."
            elif groups is not None and not all(isinstance(group, Group) for group in dep_groups):
                assert False, "Unrecognized group dependent object"

        # check dependents to prevent illegal dependent object being passed in
        dependent_check(dep_datasets, dep_groups)

        # Dataset as container
        if isinstance(dep_container, Dataset):
            container = Dataset(**dep_container.__dict__)
            dependency_metadata = {"dependentType": dep_type}
            ds_dependents = []
            grp_dependents = []

            if dep_datasets is not None and all(isinstance(dataset, Dataset) for dataset in dep_datasets):
                ds_dependents = self.get_dependent_id(dep_datasets)
                dependency_metadata["dependents"] = str(ds_dependents)

            if dep_groups is not None and all(isinstance(group, Group) for group in dep_groups):
                grp_dependents = self.get_dependent_id(dep_groups)
                dependency_metadata["dependentGroups"] = str(grp_dependents)

            if hasattr(container, "versionMetadata"):
                # if container has versionMetadata field
                # Get dependency metadata from container
                vmd = dict(container.versionMetadata)

                if "{}.dataset".format(dep_type) in vmd.keys():
                    # if adding dependents of the same dependent type:
                    # merge dependents of same type
                    update_dependentType = dep_type
                    update_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))
                    dependents = list(set(ds_dependents + update_dependents))
                    dependency_metadata["dependents"] = str(dependents)

                if "{}.group".format(dep_type) in vmd.keys():
                    # if adding dependents of the same dependent type:
                    # merge dependents of same type
                    update_dependentType = dep_type
                    update_dependents = convert_dependent_to_list(vmd.get("{}.group".format(dep_type)))
                    dependents = list(set(grp_dependents + update_dependents))
                    dependency_metadata["dependentGroups"] = str(dependents)

                container.versionMetadata.update(dependency_metadata)
            else:
                # if container doesn't have metadata field
                # provide dependency metadata as new metadata field
                container.versionMetadata = dependency_metadata

            # patching with patchds()
            try:
                ret = self.patchds(path=dep_container.path, dataset=container, versionId=dep_container.versionId)
            except:
                assert False, "Failed to add dependents"

            return ret

        # Group as Container
        elif isinstance(dep_container, Group):
            container = Group(**dep_container.__dict__)
            dependency_metadata = {"dependentType": dep_type}
            ds_dependents = []
            grp_dependents = []

            if dep_datasets is not None and all(isinstance(dataset, Dataset) for dataset in dep_datasets):
                ds_dependents = self.get_dependent_id(dep_datasets)
                dependency_metadata["dependents"] = str(ds_dependents)

            if dep_groups is not None and all(isinstance(group, Group) for group in dep_groups):
                grp_dependents = self.get_dependent_id(dep_groups)
                dependency_metadata["dependentGroups"] = str(grp_dependents)

            if hasattr(container, "metadata"):
                # if container has metadata field
                # Get dependency metadata from container
                vmd = dict(container.metadata)

                if "{}.dataset".format(dep_type) in vmd.keys():
                    # if adding dependents of the same dependent type:
                    # merge dependents of same type
                    update_dependentType = dep_type
                    update_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))
                    dependents = list(set(ds_dependents + update_dependents))
                    dependency_metadata["dependents"] = str(dependents)

                if "{}.group".format(dep_type) in vmd.keys():
                    # if adding dependents of the same dependent type:
                    # merge dependents of same type
                    update_dependentType = dep_type
                    update_dependents = convert_dependent_to_list(vmd.get("{}.group".format(dep_type)))
                    dependents = list(set(grp_dependents + update_dependents))
                    dependency_metadata["dependentGroups"] = str(dependents)

                container.metadata.update(dependency_metadata)
            else:
                # if container doesn't have metadata field
                # provide dependency metadata as new metadata field
                metadata = Metadata()
                metadata.update(dependency_metadata)
                container.metadata = metadata

            # patching with patchdir()
            try:
                ret = self.patchdir(path=container.path, container=container, type="group")
            except:
                assert False, "Failed to add dataset dependents."
            return ret
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

        def dependent_check(datasets, groups):

            if datasets is None and groups is None:
                assert False, "No dependents found."
            elif datasets is not None and not all(isinstance(dataset, Dataset) for dataset in dep_datasets):
                assert False, "Unrecognized dataset dependent object."
            elif groups is not None and not all(isinstance(group, Group) for group in dep_groups):
                assert False, "Unrecognized group dependent object"

        # check dependents to prevent illegal dependent object being passed in
        dependent_check(dep_datasets, dep_groups)

        if isinstance(dep_container, Dataset):

            container = Dataset(**dep_container.__dict__)
            remove_dataset_dependents = []
            remove_group_dependents = []

            # Ensure dep_groups or dep_datasets presents and is group object

            if dep_datasets is not None and all(isinstance(datasets, Dataset) for datasets in dep_datasets):
                remove_dataset_dependents = self.get_dependent_id(dep_datasets)

            if dep_groups is not None and all(isinstance(group, Group) for group in dep_groups):
                remove_group_dependents = self.get_dependent_id(dep_groups)

            # Ensure container group has versionMetadata and versionPk field
            if all(hasattr(container, attr) for attr in ["versionMetadata", "versionPk"]):
                vmd = dict(container.versionMetadata)
                update_dependentType = dep_type
                update_dataset_dependents = []
                update_group_dependents = []

                # Ensure dependent type provided by user is present in container group
                if "{}.dataset".format(dep_type) in vmd.keys():
                    update_dataset_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))

                if "{}.group".format(dep_type) in vmd.keys():
                    update_group_dependents = convert_dependent_to_list(vmd.get("{}.group".format(dep_type)))

                if "{}.group".format(dep_type) not in vmd.keys() and "{}.dataset".format(dep_type) not in vmd.keys():
                    raise ValueError("No dependents of type {} found".format(dep_type))

                # Ensure user provided dependents are in container
                check_exist(update_dataset_dependents, remove_dataset_dependents)
                check_exist(update_group_dependents, remove_group_dependents)

                # Step 1: remove dependents provided by users from container

                for dependent in remove_dataset_dependents:
                    update_dataset_dependents.remove(dependent)

                for dependent in remove_group_dependents:
                    update_group_dependents.remove(dependent)

                # Step 2: update dependency metadata with new dependents

                # if user removes all dependents
                # construct empty string dependency metadata
                if len(update_dataset_dependents) == 0:
                    update_dataset_dependents = ""

                if len(update_group_dependents) == 0:
                    update_group_dependents = ""

                container.versionMetadata = {"dependentGroups": str(update_group_dependents),
                                             "dependents": str(update_dataset_dependents),
                                             "dependentType": update_dependentType
                                             }

                # use patchdir() to patch group container
                try:
                    ret = self.patchds(path=dep_container.path, dataset=container,
                                       versionId=dep_container.versionId)
                except:
                    assert False, "Failed to remove dependents"
            else:
                raise ValueError("Dataset container doesn't have version metadata.")

            return ret

        elif isinstance(dep_container, Group):

            container = Group(**dep_container.__dict__)
            remove_dataset_dependents = []
            remove_group_dependents = []

            # Ensure dep_groups or dep_datasets presents and is group object

            if dep_datasets is not None and all(isinstance(datasets, Dataset) for datasets in dep_datasets):
                remove_dataset_dependents = self.get_dependent_id(dep_datasets)

            if dep_groups is not None and all(isinstance(group, Group) for group in dep_groups):
                remove_group_dependents = self.get_dependent_id(dep_groups)

            # Ensure container group has versionMetadata and versionPk field
            if all(hasattr(container, attr) for attr in ["metadata"]):
                vmd = dict(container.metadata)
                update_dependentType = dep_type
                update_dataset_dependents = []
                update_group_dependents = []

                # Ensure dependent type provided by user is present in container group
                if "{}.dataset".format(dep_type) in vmd.keys():
                    update_dataset_dependents = convert_dependent_to_list(vmd.get("{}.dataset".format(dep_type)))

                if "{}.group".format(dep_type) in vmd.keys():
                    update_group_dependents = convert_dependent_to_list(vmd.get("{}.group".format(dep_type)))

                if "{}.group".format(dep_type) not in vmd.keys() and "{}.dataset".format(dep_type) not in vmd.keys():
                    raise ValueError("No dependents of type {} found".format(dep_type))

                # Ensure user provided dependents are in container
                check_exist(update_dataset_dependents, remove_dataset_dependents)
                check_exist(update_group_dependents, remove_group_dependents)

                # Step 1: remove dependents provided by users from container

                for dependent in remove_dataset_dependents:
                    update_dataset_dependents.remove(dependent)

                for dependent in remove_group_dependents:
                    update_group_dependents.remove(dependent)

                # Step 2: update dependency metadata with new dependents

                # if user removes all dependents
                # construct empty string dependency metadata
                if len(update_dataset_dependents) == 0:
                    update_dataset_dependents = ""

                if len(update_group_dependents) == 0:
                    update_group_dependents = ""

                container.metadata = {"dependentGroups": str(update_group_dependents),
                                      "dependents": str(update_dataset_dependents),
                                      "dependentType": update_dependentType
                                      }

                # use patchdir() to patch group container
                try:
                    ret = self.patchdir(path=container.path, container=container, type="group")
                except:
                    assert False, "Failed to remove dependents"
            else:
                raise ValueError("Group container doesn't have metadata.")

            return ret
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
