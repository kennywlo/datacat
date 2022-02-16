import copy
from .model import Dataset, Group, Metadata


# noinspection PyPep8Naming,PyShadowingBuiltins,PyUnusedLocal
class ClientHelper(object):
    """
    Pythonic Client Helper for the Client interacting with the data catalog.
    """

    def __init__(self, parent):
        self.parent = parent
        self.dependency_cache = {}
        self.remaining_chunk_size = 0
        self.currentDepth = 0
        self.process_queue = None
        self.dep_name = None
        self.currentDependency = 0
        self.dependents_retrieved_current_level = 0
        self.dependents_retrieved_current_container = None
        self.dep_type = None
        self.max_depth = 0

    def get_dependent_id(self, dependents):
        """
        Fetch the identifiers used for dataset dependency.
        :param dependents: one or more dependents, of Dataset or Group
        :return: the identifiers of the input dependents
        """
        if not dependents:
            return None

        if isinstance(dependents, Dataset):
            return dependents.versionPk
        elif isinstance(dependents, Group):
            return dependents.pk

        ids = []
        for dep in dependents:
            if isinstance(dep, Dataset):
                if hasattr(dep, "versionPk"):
                    ids.append(dep.versionPk)
                else:
                    raise ValueError("Could not retrieve the versionPk of the dataset dependent.")
            elif isinstance(dep, Group):
                if hasattr(dep, "pk"):
                    ids.append(dep.pk)
                else:
                    raise ValueError("Could not retrieve the pk of the group dependent")
            else:
                raise ValueError("Unrecognized dependent type")
        return ids

    def encodeForCache(self, ToEncode):

        if not ToEncode:
            return []

        if isinstance(ToEncode, str):
            versionPK_list = ToEncode.split(",")
            map_list = map(int, versionPK_list)
            list_of_versionPKs = list(map_list)
            return list_of_versionPKs

        containerVersionPk = self.get_dependent_id(ToEncode)

        if isinstance(containerVersionPk, list):
            return containerVersionPk
        else:
            listConversion = [containerVersionPk]
            return listConversion

    def decodeFromCache(self, ToDecode, type):

        if not ToDecode or ToDecode == [None]:
            return []

        try:
            stringConversion = [str(element) for element in ToDecode]
            ToDecodeCommaDelimited = ",".join(stringConversion)
        except Exception as e:
            print(e)

        decodeResults = []
        if type == "dataset":
            decodeResults = self.parent.search(target=self.dep_name,
                                               show="dependents",
                                               query='dependents in ({})'.format(ToDecodeCommaDelimited),
                                               ignoreShowKeyError=True)
        elif type == "group":
            decodeResults = self.parent.search(target=self.dep_name,
                                               show="dependency.groups",
                                               containerFilter='dependentGroups in ({})'.format(ToDecodeCommaDelimited),
                                               ignoreShowKeyError=True)

        return decodeResults

    def retrieveContainerDependents(self, dep_container_processed):
        """
        Method which retrieves dependents from a provided container (is to be used recursively)
        :param dep_container_processed: Parent container object you wish to get dependents from.
        :return: List of dependents object attached to parent container
        """

        # *** "CC_Dependents" stands for "Current Container Dependents" ***
        CC_Dependents = []
        CC_Dependents_Group = []

        # The provided container is a Dataset
        # if isinstance(dep_container_processed, Dataset):
        try:
            if self.currentDepth >= 0:

                dependentsToRetrieve, dependentsToRetrieve_Group = None, None
                if isinstance(dep_container_processed, Dataset):
                    try:
                        dependentsToRetrieve = dep_container_processed.versionMetadata[self.dep_type + '.dataset']
                    except:
                        pass
                    try:
                        dependentsToRetrieve_Group = dep_container_processed.versionMetadata[self.dep_type + '.group']
                    except:
                        pass

                    if not dependentsToRetrieve and not dependentsToRetrieve_Group:
                        return [], []

                elif isinstance(dep_container_processed, Group):
                    try:
                        dependentsToRetrieve = dep_container_processed.metadata.dct[self.dep_type + '.dataset']
                    except:
                        pass
                    try:
                        dependentsToRetrieve_Group = dep_container_processed.metadata.dct[self.dep_type + '.group']
                    except:
                        pass

                    if not dependentsToRetrieve and not dependentsToRetrieve_Group:
                        return [], []

                else:
                    raise ValueError("Unrecognized dependency container")
        except:
            if self.currentDepth > 0:
                pass
            else:
                return [], []

        # Searching for all dataset dependents
        searchResults = []
        try:
            if dependentsToRetrieve:
                searchResults = self.parent.search(target=self.dep_name,
                                                   show="dependents",
                                                   query='dependents in ({})'.format(dependentsToRetrieve),
                                                   ignoreShowKeyError=True)
        except:
            pass

        # Searching for all GROUP dependents
        searchResults_Groups = []
        try:
            if dependentsToRetrieve_Group:
                searchResults_Groups = self.parent.search(target=self.dep_name,
                                                          show="dependency.groups",
                                                          containerFilter='dependentGroups in ({})'
                                                          .format(dependentsToRetrieve_Group),
                                                          ignoreShowKeyError=True)
        except:
            pass

        if not searchResults and not searchResults_Groups:
            return [], []

        # Creating 2 separate queues, one for dataset and one for GROUP
        dependent_queue_Dataset = copy.deepcopy(searchResults)
        dependent_queue_Group = copy.deepcopy(searchResults_Groups)

        # For all the search results (DATASET), process them
        try:
            for dependent in searchResults:

                # We have reached the chunk size limit, return dependents for this container
                if self.remaining_chunk_size <= 0:

                    # Convert to versionPK list - all_dependents_for_current_container_datasets
                    datasets_to_retrieve_enc = self.encodeForCache(dependentsToRetrieve)
                    self.dependency_cache[self.dep_name][
                        "all_dependents_for_current_container_datasets"] = datasets_to_retrieve_enc

                    # Convert to versionPK list - dependents_left_datasets
                    datasets_left_enc = self.encodeForCache(dependent_queue_Dataset)
                    self.dependency_cache[self.dep_name]["dependents_left_datasets"].extend(datasets_left_enc)

                    return CC_Dependents, CC_Dependents_Group

                # We have available chunks:
                else:
                    CC_Dependents.append(dependent)

                    # Convert to versionPK list - dependents_retrieved_so_far_datasets
                    datasets_retrieved_enc = self.encodeForCache(dependent)
                    self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_datasets"]\
                        .extend(datasets_retrieved_enc)

                    dependent_queue_Dataset.pop(0)
                    self.remaining_chunk_size -= 1
        except:
            pass

        # For all the search results (DATASET), process them
        try:
            for dependent in searchResults_Groups:

                # We have reached the chunk size limit, return dependents for this container
                if self.remaining_chunk_size <= 0:

                    # Convert to versionPK list - all_dependents_for_current_container_group
                    groups_to_retrieve_enc = self.encodeForCache(dependentsToRetrieve_Group)
                    self.dependency_cache[self.dep_name][
                        "all_dependents_for_current_container_group"] = groups_to_retrieve_enc

                    # Convert to versionPK list - dependents_left_group
                    groups_left_enc = self.encodeForCache(dependent_queue_Group)
                    self.dependency_cache[self.dep_name]["dependents_left_group"].extend(groups_left_enc)

                    return CC_Dependents, CC_Dependents_Group

                # We have available chunks:
                else:
                    CC_Dependents_Group.append(dependent)

                    # Convert to versionPK list - dependents_retrieved_so_far_datasets
                    groups_retrieved_enc = self.encodeForCache(dependent)
                    self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_groups"]\
                        .extend(groups_retrieved_enc)

                    dependent_queue_Group.pop(0)
                    self.remaining_chunk_size -= 1
        except:
            pass

        # Return the retrieved dependents
        return CC_Dependents, CC_Dependents_Group

    def retrieveContainerDependentsNext(self, dep_container_processed):
        """
        Method which retrieves dependents from a provided container (is to be used recursively)
        :param dep_container_processed: Parent container object you wish to get dependents from.
        :return: List of dependents object attached to parent container
        """
        dependents_dataset = []
        dependents_group = []

        if isinstance(dep_container_processed, Dataset) or isinstance(dep_container_processed, Group):

            # Retrieve and decode the following from cache
            # - Dependents Left (Datasets)
            # - Dependents Left (Groups)
            # If "None" is returned for either, replace with an
            dependentsToRetrieve = str(self.get_dependent_id(self.decodeFromCache(
                self.currentDependency.get("dependents_left_datasets"), "dataset")))
            if dependentsToRetrieve == "None":
                dependentsToRetrieve = "[]"
            dependentsToRetrieve = dependentsToRetrieve.replace('[', '')
            dependentsToRetrieve = dependentsToRetrieve.replace(']', '')

            dependentsToRetrieveGroups = str(self.get_dependent_id(self.decodeFromCache(
                self.currentDependency.get("dependents_left_groups"), "group")))
            if dependentsToRetrieveGroups == "None":
                dependentsToRetrieveGroups = "[]"
            dependentsToRetrieveGroups = dependentsToRetrieveGroups.replace('[', '')
            dependentsToRetrieveGroups = dependentsToRetrieveGroups.replace(']', '')

            # if we have retrieved no dependents and have no dependents to retrieve, get dependents to retrieve
            if not self.dependents_retrieved_current_container:
                if self.currentDepth >= 0:
                    try:
                        if isinstance(dep_container_processed, Dataset):
                            dependentsToRetrieve = dep_container_processed.versionMetadata.dct[self.dep_type
                                                                                               + '.dataset']
                        elif isinstance(dep_container_processed, Group):
                            dependentsToRetrieve = dep_container_processed.metadata.dct[self.dep_type + '.dataset']
                    except:
                        pass
                    try:
                        if isinstance(dep_container_processed, Dataset):
                            dependentsToRetrieveGroups = dep_container_processed.versionMetadata.dct[self.dep_type
                                                                                                     + '.group']
                        elif isinstance(dep_container_processed, Group):
                            dependentsToRetrieve = dep_container_processed.metadata.dct[self.dep_type + '.group']

                    except:
                        pass

                self.dependency_cache[self.dep_name][
                    "all_dependents_for_current_container_datasets"] = dependentsToRetrieve

                self.dependency_cache[self.dep_name][
                    "all_dependents_for_current_container_groups"] = dependentsToRetrieveGroups

            converted_list_datasets = [str(element) for element in
                                       self.currentDependency.get('dependents_retrieved_so_far_datasets')]
            joined_string_datasets = ",".join(converted_list_datasets)

            converted_list_groups = [str(element) for element in
                                     self.currentDependency.get('dependents_retrieved_so_far_groups')]
            joined_string_groups = ",".join(converted_list_groups)

            if dependentsToRetrieve == joined_string_datasets:
                if dependentsToRetrieveGroups == joined_string_groups:
                    raise "Done"

            searchResults = []
            try:
                if dependentsToRetrieve:
                    searchResults = self.parent.search(target=self.dep_name,
                                                       show="dependents",
                                                       query='dependents in ({})'.format(dependentsToRetrieve),
                                                       ignoreShowKeyError=True)
            except:
               pass

            searchResults_group = []
            try:
                if dependentsToRetrieveGroups:
                    searchResults_group = self.parent.search(target=self.dep_name,
                                                             show="dependency.groups",
                                                             containerFilter='dependentGroups in ({})'
                                                             .format(dependentsToRetrieveGroups),
                                                             ignoreShowKeyError=True)
            except:
                pass

            if searchResults == [] and searchResults_group == []:
                return [], []

            dependent_queue_dataset = copy.deepcopy(searchResults)
            dependent_queue_group = copy.deepcopy(searchResults_group)

            try:
                for dependent in searchResults:

                    # We have reached the chunk size limit, return dependents for this container
                    if self.remaining_chunk_size <= 0:
                        # Encode to versionPK list - dependents_left_datasets
                        dep_datasets_left_enc = self.encodeForCache(dependent_queue_dataset)
                        self.dependency_cache[self.dep_name]["dependents_left_datasets"] = dep_datasets_left_enc
                        return dependents_dataset, dependents_group

                    # We have available chunks remaining:
                    else:
                        dependents_dataset.append(dependent)

                        # Encode to versionPK list - dependents_retrieved_so_far_datasets
                        dep_datasets_retrieved_enc = self.encodeForCache(dependent)
                        self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_datasets"]\
                            .extend(dep_datasets_retrieved_enc)
                        dependent_queue_dataset.pop(0)
                        dependents_left_enc = self.encodeForCache(dependent_queue_dataset)
                        self.dependency_cache[self.dep_name]['dependents_left_datasets'] = dependents_left_enc
                        self.remaining_chunk_size -= 1
            except:
                pass

            try:
                for dependent in searchResults_group:

                    # We have reached the chunk size limit, return dependents for this container
                    if self.remaining_chunk_size <= 0:

                        # Convert to versionPK list - dependents_left_groups
                        dep_groups_left_enc = self.encodeForCache(dependent_queue_group)
                        self.dependency_cache[self.dep_name]["dependents_left_groups"].extend(dep_groups_left_enc)

                        return dependents_dataset, dependents_group

                    # We have available chunks remaining:
                    else:
                        dependents_group.append(dependent)

                        # Convert to versionPK list - dependents_retrieved_so_far_groups
                        dep_groups_retrieved_enc = self.encodeForCache(dependent)
                        self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_groups"]\
                            .extend(dep_groups_retrieved_enc)

                        dependent_queue_group.pop(0)

                        dep_groups_left_enc = self.encodeForCache(dependent_queue_group)
                        self.dependency_cache[self.dep_name]['dependents_left_groups'] = dep_groups_left_enc
                        self.remaining_chunk_size -= 1
            except:
                pass

            # Return the retrieved dependents
            return dependents_dataset, dependents_group

        # The provided container is neither a Dataset nor a Group
        else:
            raise ValueError("Unrecognized dependency container")

    def get_dependents(self, dep_container, dep_type, max_depth, chunk_size):

        # Retrieve and store the dependencyName of current container
        try:
            if hasattr(dep_container, "versionMetadata"):
                self.dep_name = dep_container.versionMetadata["dependencyName"]
            elif hasattr(dep_container, "metadata"):
                self.dep_name = dep_container.metadata["dependencyName"]
            else:
                print("provided container has no metadata")
                return []
        except:
            print("provided container has no dependency information associated")
            return []

        # Variable declaration
        self.currentDepth = 0  # Current depth should always start at 1.
        containersToProcess = []  # The containers we will be retrieving dependencies from.
        containersToProcess_Dataset = []    # Containers we still need to process - Datasets
        containersToProcess_Group = []  # Containers we still need to process - Groups

        nextContainersToProcess = []  # The next level of containers to process, both DS and GROUPS.
        nextContainersToProcess_Dataset = []
        nextContainersToProcess_Group = []
        retrieved_dependents = []  # arraylist for returned dependents to be stored

        # Will keep track of all the DATASET dependents already retrieved
        dependents_already_retrieved = []

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
        if self.dep_name in self.dependency_cache.keys():
            print("Error in method call: .get_dependents")
            print("Search process already started, please use get_next_dependency method call to continue retrieval")
            return []
        else:
            self.dependency_cache[self.dep_name] = dependency_information

        # Save the entire dependency object to further process
        self.dep_type = dep_type
        self.max_depth = max_depth
        self.currentDependency = self.dependency_cache.get(self.dep_name)
        self.remaining_chunk_size = self.currentDependency.get("chunk_size")

        # Create a list for the two types of containers to be processed - Dataset and Group
        if isinstance(dep_container, Dataset):
            containersToProcess_Dataset.append(dep_container)
        if isinstance(dep_container, Group):
            containersToProcess_Group.append(dep_container)

        # Create a combined list of the two types - Dataset and Group
        containersToProcess.append(dep_container)
        # Create an unreferenced copy of the created list
        self.process_queue = copy.deepcopy(containersToProcess)

        # Iterates through each level starting at level 1 ( index 0 ) and ending at the user providing max_depth.
        for self.currentDepth in range(max_depth):
            # Retrieve dependents from each of the containers in containersToProcess
            next_container, next_container_Group = [], []
            for container in containersToProcess:
                try:
                    try:
                        # returns 2 values, datasets retrieved and groups retrieved
                        next_container, next_container_Group = self.retrieveContainerDependents(container)

                        nextContainersToProcess.extend(next_container + next_container_Group)
                        nextContainersToProcess_Dataset.extend(next_container)
                        nextContainersToProcess_Group.extend(next_container_Group)

                        retrieved_dependents.extend(next_container + next_container_Group)
                    except Exception as e:
                        del self.process_queue[0]
                        continue

                    # if we have not exceeded the chunk limit
                    if self.remaining_chunk_size > 0:

                        if isinstance(container, Dataset):
                            containersToProcess_Dataset.remove(container)
                        elif isinstance(container, Group):
                            containersToProcess_Group.remove(container)

                    # if we have exceeded the chunk limit
                    if self.remaining_chunk_size <= 0:

                        self.dependency_cache[self.dep_name]["current_depth"] = self.currentDepth

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        datasets_to_process_enc = self.encodeForCache(containersToProcess_Dataset)
                        self.dependency_cache[self.dep_name]["all_containers_at_current_depth_datasets"]\
                            .extend(datasets_to_process_enc)
                        groups_to_process_enc = self.encodeForCache(containersToProcess_Group)
                        self.dependency_cache[self.dep_name]["all_containers_at_current_depth_groups"]\
                            .extend(groups_to_process_enc)

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        datasets_left_enc = self.encodeForCache(containersToProcess_Dataset)
                        self.dependency_cache[self.dep_name]["containers_left_to_process_datasets"]\
                            .extend(datasets_left_enc)
                        groups_left_enc = self.encodeForCache(containersToProcess_Group)
                        self.dependency_cache[self.dep_name]["containers_left_to_process_groups"]\
                            .extend(groups_left_enc)

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
            containersToProcess_Dataset = dependents_already_retrieved + nextContainersToProcess_Dataset
            containersToProcess_Group = dependents_already_retrieved + nextContainersToProcess_Group

            # Clear current level of dependents already retrieved
            # Clear queue
            dependents_already_retrieved.clear()
            self.process_queue.clear()

            # Clear the cache
            self.dependency_cache[self.dep_name]["containers_left_to_process_datasets"] = []
            self.dependency_cache[self.dep_name]["containers_left_to_process_groups"] = []

            self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_datasets"] = []
            self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_groups"] = []

            if (not nextContainersToProcess_Dataset and not nextContainersToProcess_Group) \
                    or self.currentDepth == (max_depth-1):
                print("Finished Processing the following dependency --> ", self.dep_name,
                      "\nReturning last batch of dependents, if any... ")
                self.dependency_cache.pop(self.dep_name)
                return retrieved_dependents
            else:
                nextContainersToProcess.clear()
                nextContainersToProcess_Dataset.clear()
                nextContainersToProcess_Group.clear()
        return retrieved_dependents

    def get_next_dependents(self, dep_container):
        try:
            if hasattr(dep_container, "versionMetadata"):
                self.dep_name = dep_container.versionMetadata["dependencyName"]
            elif hasattr(dep_container, "metadata"):
                self.dep_name = dep_container.metadata["dependencyName"]
            else:
                print("provided container has no metadata")
                return []
        except:
            print("provided container has no dependency information associated")
            return []

        self.currentDependency = self.dependency_cache.get(self.dep_name)

        nextContainersToProcess = []
        next_container_Dataset = []
        next_container_Group = []
        retrieved_dependents = []

        if self.currentDependency is None:
            print("Dependency (" + self.dep_name + "): No  dependents for that container!")
            return []

        self.dep_type = self.currentDependency.get("dep_type")
        self.max_depth = self.currentDependency.get("max_depth")
        current_depth = self.currentDependency.get("current_depth")
        self.remaining_chunk_size = self.currentDependency.get("chunk_size")

        all_containers_at_current_depth = self.decodeFromCache(self.currentDependency.get(
            "all_containers_at_current_depth_datasets"), "dataset") + \
            self.decodeFromCache(self.currentDependency.get("all_containers_at_current_depth_groups"), "group")

        containersToProcess = self.decodeFromCache(self.currentDependency.get("containers_left_to_process_datasets"),
                                                   "dataset") + self.decodeFromCache(self.currentDependency.get(
                                                   "containers_left_to_process_groups"), "group")

        containersToProcess_Dataset = self.decodeFromCache(self.currentDependency.get(
            "containers_left_to_process_datasets"), "dataset")
        containersToProcess_Group = self.decodeFromCache(self.currentDependency.get(
            "containers_left_to_process_groups"), "group")
        self.process_queue = copy.deepcopy(containersToProcess)

        self.dependents_retrieved_current_container = self.decodeFromCache(self.currentDependency.get(
                                                 "dependents_retrieved_so_far_datasets"), "dataset") + \
                                                 self.decodeFromCache(self.currentDependency.get(
                                                 "dependents_retrieved_so_far_groups"), "group")

        self.dependents_retrieved_current_level = self.decodeFromCache(self.currentDependency.get(
                                             "dependents_retrieved_so_far_datasets"), "dataset") + \
                                             self.decodeFromCache(self.currentDependency.get(
                                             "dependents_retrieved_so_far_groups"), "group")

        # Iterates through each level, resuming according to cache, and ending at the user providing max_depth.
        for self.currentDepth in range(self.max_depth)[current_depth:]:
            # Iterate through each container stored in containersToProcess
            for container in containersToProcess:
                try:
                    try:
                        # For each container retrieve its dependents
                        next_container_Dataset, next_container_Group = self.retrieveContainerDependentsNext(container)

                        self.dependents_retrieved_current_level.append(next_container_Dataset + next_container_Group)
                        retrieved_dependents.extend(next_container_Dataset + next_container_Group)
                        nextContainersToProcess.extend(next_container_Dataset + next_container_Group)

                    except:
                        del self.process_queue[0]

                        if isinstance(container, Dataset):
                            del containersToProcess_Dataset[0]
                        elif isinstance(container, Group):
                            del containersToProcess_Group[0]

                        self.dependents_retrieved_current_container.clear()
                        continue

                    if self.remaining_chunk_size > 0:
                        del self.process_queue[0]

                        if isinstance(container, Dataset):
                            del containersToProcess_Dataset[0]
                        elif isinstance(container, Group):
                            del containersToProcess_Group[0]

                        self.dependents_retrieved_current_container.clear()

                    if self.remaining_chunk_size <= 0:
                        self.dependency_cache[self.dep_name]["current_depth"] = self.currentDepth

                        temp_containersToProcess_Dataset = []
                        temp_containersToProcess_Group = []

                        for x in containersToProcess:
                            if isinstance(x, Dataset):
                                temp_containersToProcess_Dataset.append(x)
                            elif isinstance(x, Group):
                                temp_containersToProcess_Group.append(x)

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        containers_to_process_Dataset_enc = self.encodeForCache(temp_containersToProcess_Dataset)
                        self.dependency_cache[self.dep_name]["all_containers_at_current_depth_datasets"] \
                            = containers_to_process_Dataset_enc
                        containers_to_process_Group_enc = self.encodeForCache(temp_containersToProcess_Group)
                        self.dependency_cache[self.dep_name]["all_containers_at_current_depth_groups"] \
                            = containers_to_process_Group_enc

                        # Convert to versionPK list then store in cache, for both groups and datasets
                        containers_to_process_Dataset_enc = self.encodeForCache(containersToProcess_Dataset)
                        self.dependency_cache[self.dep_name]["containers_left_to_process_datasets"] \
                            = containers_to_process_Dataset_enc
                        containers_to_process_Group_enc = self.encodeForCache(containersToProcess_Group)
                        self.dependency_cache[self.dep_name]["containers_left_to_process_groups"] \
                            = containers_to_process_Group_enc

                        return retrieved_dependents
                except:
                    pass

            # Prepare to process next level of containers
            containersToProcess.clear()
            containersToProcess_Dataset.clear()
            containersToProcess_Group.clear()

            self.dependents_retrieved_current_level.clear()
            self.dependents_retrieved_current_container.clear()
            nextContainersToProcess.clear()

            # Set next level of containers to process
            # Make a freely mutable copy of that list
            containersToProcess = self.decodeFromCache(self.currentDependency.get(
                    "dependents_retrieved_so_far_datasets"), "dataset") + self.decodeFromCache(
                self.currentDependency.get("dependents_retrieved_so_far_groups"), "group")
            containersToProcess_Dataset = self.decodeFromCache(self.currentDependency.get(
                "dependents_retrieved_so_far_datasets"), "dataset")
            containersToProcess_Group = self.decodeFromCache(self.currentDependency.get(
                "dependents_retrieved_so_far_groups"), "group")

            self.process_queue = copy.deepcopy(containersToProcess)

            self.dependency_cache[self.dep_name]["containers_left_to_process_datasets"] = []
            self.dependency_cache[self.dep_name]["containers_left_to_process_groups"] = []

            self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_datasets"] = []
            self.dependency_cache[self.dep_name]["dependents_retrieved_so_far_groups"] = []

            self.dependency_cache[self.dep_name]['all_containers_at_current_depth_datasets'] = []
            self.dependency_cache[self.dep_name]['all_containers_at_current_depth_groups'] = []

        if not containersToProcess or self.currentDepth == (self.max_depth-1):
            print("Finished Processing the following dependency --> ", self.dep_name,
                  "\nReturning last batch of dependents, if any... ")
            self.dependency_cache.pop(self.dep_name)
            return retrieved_dependents
        else:
            nextContainersToProcess.clear()

        return retrieved_dependents

    def add_dependents(self, dep_container, dep_type, dep_datasets=None, dep_groups=None, **kwargs):
        """
         Attach new dependents to container object.
        :param dep_container: Parent container object to add dependents to
        :param dep_type: Type of dependents to add
        :param dep_datasets: The datasets we wish to use as children of the parent container.
        VersionPKs are required for each dependent dataset.
        :param dep_groups: The groups we wish to use as children of the parent container
        """
        patchds = kwargs.get('patchds')
        patchdir = kwargs.get('patchdir')

        if not dep_datasets and not dep_groups:
            return None

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
                ret = patchds(path=dep_container.path, dataset=container, versionId=dep_container.versionId)
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
                ret = patchdir(path=container.path, container=container, type="group")
            except:
                assert False, "Failed to add dataset dependents."
            return ret
        else:
            raise ValueError("Unrecognized dependency container")

    def remove_dependents(self, dep_container, dep_type, dep_datasets=None, dep_groups=None, **kwargs):
        """
        Remove dependents from container object provided
        :param dep_container: Parent container object to remove dependents from
        :param dep_type: Type of dependents to remove
        :param dep_datasets: The datasets we wish to remove from the parent container
        :param dep_groups: The groups we wish to remove from the parent container
        """
        patchds = kwargs.get('patchds')
        patchdir = kwargs.get('patchdir')

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

                # use patchds() to patch dataset container
                try:
                    ret = patchds(path=dep_container.path, dataset=container,
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
                    ret = patchdir(path=container.path, container=container, type="group")
                except:
                    assert False, "Failed to remove dependents"
            else:
                raise ValueError("Group container doesn't have metadata.")

            return ret
        else:
            raise ValueError("Unrecognized dependency container")