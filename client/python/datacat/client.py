
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
    def get_dependents(self, dep_container, dep_type, max_depth=2, chunk_size=100):
        """
        Retrieve dependents up to chunk_size at a time, subject to max_depth
        :param dep_container: Parent container object you wish to get dependents from.
        :param dep_type: Type of dependents to get
        :param max_depth: Depth of dependency chain
        :param chunk_size: Total amount of dependents retrieved
        :return: List of dependents object attached to parent container
        """

        # Check to see if container has any dependents that are of type group, if it does then chances are that it will
        # have many levels we need to traverse.


        currentDepth = 1
        containersToProcess = []
        newContainersToProcess = []

        containersToProcess.append(dep_container)
        dependents = []

        def retrieveContainerDependents(dep_container_processed):
            if isinstance(dep_container_processed, Dataset):

                container_dependency_name = dep_container_processed.versionMetadata["dependencyName"]
                dependentsToRetrieve = dep_container_processed.versionMetadata[dep_type + '.dataset']

                for dataset in self.search(target=container_dependency_name, show="dependents", query='dependents in ({''})'.format(dependentsToRetrieve), ignoreShowKeyError=True):
                    self.path(path=dataset.path + ";v=" + str(dataset.versionId))
                    dependents.append(dataset)

                return dependents

            elif isinstance(dep_container_processed, Group):
                # TODO: get dependents from the group dependency, to be cached
                return dependents
            else:
                raise ValueError("Unrecognized dependency container")

        # Iterates through each level starting at level 1.
        for currentDepth in range(max_depth):
            for unprocessedContainer in containersToProcess:
                try:
                    newContainersToProcess.extend(retrieveContainerDependents(unprocessedContainer))
                except:
                    print("No dependency found for this container")
            containersToProcess.clear()
            containersToProcess = newContainersToProcess

        return dependents


    @checked_error
    def get_next_dependents(self, dep_container):
        """
         Retrieve next dependents attached to container object.
        :param dep_container: Parent container object you wish to get next dependents from
        :return: List of dependent objects attached to container object
        """
        dependents = []
        if isinstance(dep_container, Dataset):
            # TODO: get next dependents from the dataset dependency in cache, to be removed when empty
            return dependents
        elif isinstance(dep_container, Group):
            # TODO: get next dependents from the group dependency in cache, to be removed when empty
            return dependents
        else:
            raise ValueError("Unrecognized dependency container")

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
