# Tests

There are two kinds of test documented in this file. The first one is the [**Maven Test**](#maven-test-instruction) and
the second one is the [**Python Client Test**](#python-client-test-instruction).

## Maven Test Instruction

Once you have completed all of the [Prerequisites](INSTALL.md/#maven-test), you can begin testing by navigating to the
top level of the directory and using the command line `mvn test`. This will scan all the marked test directories in the
project, and runs the found tests automatically.

If you want to run a test on a specific component of the directory, you can navigate to its folder and run `mvn test`.

If you wish to run a single test, you can use the following command `mvn -Dtest=<testname> test`. Keep in mind that in
order to run a single test you need to be in the folder of the specific component you are testing.

## Reactor Build Order:

* SRS DataCatalog Main Project
* SRS DataCatalog Test Packages
* SRS DataCatalog Model
* SRS Virtual File System library
* SRS DataCatalog Default Implementation
* SRS DataCatalog Core
* SRS DataCatalog RESTful Interfaces
* SRS DataCatalog Client

## **SRS DataCatalog Main Project**

No tests to run

## **SRS DataCatalog Test Packages**

No tests to run

## **SRS DataCatalog Model**

### **DcPermissionsTest**

* **void testUnpackPack();**\
  Ensures that we can properly pack and unpack a string.

  In this test we are taking the string "adirw" and having it unpacked, one character at a time, into a hashset. The
  returned hashset for the string "adirw" would be ADMIN, DELETE, INSERT, READ, WRITE. Now that we have our permissions
  hashset, we will pack it, one permission at a time, and have the corresponding returned characters appended to a
  string. To ensure that the packing and unpacking was done correctly we finish by comparing the returned string to a
  test case of "ridwa".

  *Unpacking* a character means taking one of the following chars ('r','i','d','w','a') and returning a corresponding
  permission from the declared Enums (READ, INSERT, DELETE, WRITE, ADMIN). For example providing the char 'a' would
  return the permission ADMIN.

  *Packing* a permission means taking a provided permission and returning a corresponding character. For example
  providing the permission ADMIN would return the char 'a'.


* **void testUnpack();**\
  Ensures that we can properly unpack a single char using the unPack(); function.

  In this test we are running the unPack(); function with different parameters and making sure that the proper enum type
  permission is being returned for each parameter. This is done through the use of assertTrue, where it compares the
  value returned by unpack(); to its corresponding permission.


* **void testGetAclAttributes();**\
  Tests to make sure that we are able to retrieve specific user information, in this case the specified permissions,
  from a string containing comma separated user information.


* **void testMergeAclEntries();**\
  Tests to see that we can take two lists populated with entries of type "DcAclEntry" and combine them and their values
  into a single list.


* **void testRemoveEmptyPermissionEntries();**\
  Ensures that if given an entry with no permissions, it automatically gets removed from its associated list.

## **SRS Virtual File System library**

### **AbstractPathTest**

* **void testCreate();**\
  Tests the functionality of select methods defined in the interface `AbstractPath`, and compares it with the
  functionality of matching methods within Java's own `Path` interface. If two coresponding methods return the same
  result then we know that our created path is valid.

  The following methods are tested in order to confirm validity...

    * resolve();
    * getFileName();
    * getNameCount();
  <br>
  A formal description of AbstractPath can be found below.

  ```
    AbstractPath
    Author: bvan

    Description: An interface of Java's path class defined to fit slaclab-datacat's specific needs.
  ```

### **GlobToRegexTest**

* **void testPathGlob();**\
  Tests the methods within the class GlobToRegex to see if they can properly convert a provided glob to a regex.

  This test is done by first performing a conversion from glob to regex. We then take the returned regex and use it to
  create a pattern. Once we have our pattern we can begin to look for it through a variety of path containing strings.
  It will then display to the console if the pattern was found in the string or not.

  Example of path containing string : `String path01_11 = "/abc/b/b";`

  Not all tests will result in a match and that is expected. We are not testing for 100% matches, we are simply testing
  to see if we can take a glob, convert it to a regex, and use that regex as a pattern to search for in a variety of
  strings.

### **PathUtilsTest**

* **void testDotDot();**\
  This test ensures that we are properly able to normalize a string containing a path, using two methods...
  `.normalize()` and `.normalizeSeparators()`.

  The testing is done by repeatedly calling both the `.normalize()` and `.normalizeSeparators()` methods with a variety
  of different parameters. After every call we take the result and compare it to a predetermined result. If both the
  predetermined result, and the results returned from individual method calls are a match then we know that our method
  did indeed compute the proper normalization.

  ```
  .normalize();
   Takes a string as a parameter and returns it completely normalized.
   1). Removes all "."
   2). Removes all trailing "/"
   3). Removes all occurances of "name/.." as well as all proceeding names.

   Example:
   PathUtils.normalize("//abc//def/.") --returns-- "/abc/def"
  ```

  ```
  .normalizeSeparators();
   Takes a string as a parameter and returns it with only the separators normalized.
   In our case the separators are the "/" so all duplicate forward slashes are removed.

   Example:
   PathUtils.normalizeSeparators("//abc//def/..") --returns-- "/abc/def/.."
  ```

## **SRS DataCatalog Default Implementation**

### **DatasetSqlDAOTest**

* **void TestCreateNode();**\
  Tests the creation of a new dataset as well as the ability to insert it into the table `VerDataset`. If any exceptions
  are raised during the creation or insertion of the dataset, the test will fail.


* **void testDeleteDatasetVersion();**\
  Tests the deletion of a created/merged Dataset Version from the VerDataset table .


* **void testMetadata();**\
  Tests to make sure that the metadata within a created hashmap can be stored and accessed properly.

### **DatacatSearchTest**

* **void testSearchForDatasetsInParent();**\
  Tests to ensure that the proper datasets are being returned when a queryString is passed to the method `doSearch()`.

  The test confirms the validity of the query by checking two main things.
    * That the number of datasets returned matches the number of expected datasets
    * That the returned dataset contains data as expected by the developer. This is done through the use of an
      assertEquals statement where a select portion of the returned dataset must match a string decided on by the
      developer.
  <br>

* **void testWithSortFields();**\
  Tests to see that if provided with sort fields, we can properly search and retrieve the specified data sets while also
  having them be sorted according to the specified sort fields mentioned earlier.

  The test confirms that the proper results have been produced by comparing the number of returned datasets to a
  prefined expected number of datasets. If these two match then the test is considered successful.


* **void testErrorString();**\
  Tests to see that if given an invalid string the method will detect it and report the string as an error.

  The test goes about doing this by first providing the method `doSearch();` with the query "x == 'de'". By providing it
  with this specific query we expect the `doSearch` method to fail and return the following error message, " Unable to
  resolve 'x' ". If this message is indeed returned, the test has succeeded.

### **DatasetMetanameContextTest (No longer needed)**

* DatasetMetaName table is no longer used by Datacat, disregard this test.

### **AppTest**

* **void testApp();**\
  A single statement asserting that true = true.

### **DatacatObjectBuilderTest**

* **void testMask();**\
  Tests to see that we can properly create a new DatacatObject, with all its associated variables , by using
  the `. Builder();` method. We then test to see that we can set its variables using other methods from within the
  `DatacatObject` class.

  ```
  b.jsonType( "DATASET");
  b.pk(1234L);
  b.name("hello");
  ```

  We confirm that the methods mentioned above do in fact work by using an `assertTrue();` statement to confirm its
  content.

### **DatasetDeserialization**

* **testNoType();**
  Testing to see if we can deserialize a JSON string with no type into a variety of different JAVA types.

  The types are
    * FlatDataset

  (WORK IN PROGRESS)

### **DatasetVersionTest**

* **void testDeserialization();**
  Tests to see if we can properly take a JSON and deserialize it into `DatasetVersion.class`.

  This test takes the following steps to reach a result
    * This test starts by taking the JSON (jsonText) and deserializing it into the DataSetVersion object (o).
    * (o) is then serialized into the JSON output (writ)
        * (writ is display to the console)
    * (writ) is then compared to a JSON containing what we think the expected output should be. If they match then the
      test is successful.

### **LatestDatasetTest**

* **void testFullDataset();**\
  Tests to see if we can properly Marshall between a java object and a JSON output.

  This test takes the following steps to reach a result
    * Creates an object (db) by using `FullDataset.Builder()`
    * Sets the following properties... name, fileFormat, resource, size, runMin, runMax, latest, taskName, versionID.
    * Once the desired properties have been set, a new dataset object (ds) is created using db as its builder source.
    * The default JSON factory is constructed using `ObjectMapper mapper = new ObjectMapper();`
    * Our object (ds) is serialized into the JSON output (json) by using `mapper.writeValue`.
        * (json) is outputed to the console
    * We then, in one line, create an object of type FullDataSet (newObject) and using `mapper.readValue` deserialize (
      json) into a Java object. We then print to console its version.
    * Our object (newObject) is serialized into a JSON output (newJson)
        * (newJson) is outputed to the console
    * At the end we finally compare (json) with (newJson) using assertEquals. If the assert equals passes then we know
      that the marshalling was a success.
  <br>

* **void testFlatDataset();**


* **void testFlatDatasetXML();**


* **void testMultivaluedMap(); (Work in progress)**\
  This test takes the following steps to reach a result
    * Creates the hashmap "mmap"
        * Using `.put` it populates the map with the following values and keys
            * name
            * fileFormat
            * resource
            * size
            * runMin
            * runMax
            * taskName
            * versionId
            * versionMetadata
    * Using FlatDataset.Builder(); create an object of type Dataset
    * Create the hashmap "methods"
        * populate it with values and keys
    * Declare a mapper to use for serialization and deserialization
    * Using a for loop traverse the map and populate....
    * Stuck at what the forloop does.

### **MapperTest**

* **void testDeserialization();**\
  Ensures that serialization is working as expected.

  This test takes the following steps to reach a result
    * Uses JacksonAnnotationIntrospector and JaxbAnnotationIntrospector to set the AnnotationIntrospector for mapper.
    * Takes a string (jsonText) and deserializes it into an object of type DatacarObject called (o)
    * Serializes the object into a JSON output (writ)
    * Tests to see that the Serialized JSON output (writ) matches our expected JSON text. If it does then the tests is a
      success.

### **MetadataTest**

* **void testMetadataNoTypesDeserialize();**


* **void testMetadataDeserialize();**


* **void testMetadataDeserializeRawMap();**


* **void testMetadataSerialize();**


* **void testLong();**

## **SRS DataCatalog Core**

### **DcFileSystemProviderTest**

* **void testCacheStream();**\
  Tests to ensure that the use of `newOptimizedDirectoryStream` works as expected and allows us to properly iterate over
  the entries in a directory. It also allows us to compare the performance difference between a cached Directory Stream
  as opposed to a unCached Directory Stream.


* **void testCreateDataset();**\
  Tests to ensure that we can properly create a dataset using `provider.createDataset();`.

  The creation of the dataset is confirmed if no exceptions are thrown during the use of `provider.createDataset();`
  and `provider.delete();`


* **void testCreateDeleteDirectory();**\
  Test to see if we can properly create and delete a directory/subdirectory.

  This test takes the following steps to reach a result
    * The test first begins by creating a directory using `provider.createDirectory();`. This directory is called
      (createFolderTest).
    * It then creates a subdirectory within the previously created directory, also called (createFolderTest)
    * Attempt to delete the parent directory (createFolderTest)
        * This attempt fails as there is currently a sub-directory within the parent directory of (createFolderTest).
    * Attempt to delete the sub-directory
        * This attempt will be successful as the sub-directory is empty.
    * Attempt to delete the parent directory again
        * This attempt will now be succesfful as we have emptied the contents of the parent directory in the previous
          step.
    * Attempt to delete the parent directory once again
        * This attempt will fail as the parent directory has already been deleted.
  <br>

  Through this we can confirm that the deletion of directories does work as intended.
  <br>

* **void testDirectoryAcl();**\
  This test ensures that we are able to create new ACLs, merge them together to a target path, then be able to retrieve
  all ACL information from specified paths.

### **DirectoryWalkerTest**

* **void testWalk();**\
  This test ensures that we are properly able to walk through any given directory and return all file names when given
  certain conditions.

  We can confirm our ability to walk through directories by doing the following.
    * Declare an object of type ContainerVisitor called visitor
        * Define visitor and pass a `syntaxAndPattern` as a single parameter
            * If `syntaxAndPattern` ends with $ we do not search Groups.
            * If `syntaxAndPattern` ends with ^ we do not search Folders.
    * Declare an object of type DirectoryWalker called walker
        * Define walker and pass the following as parameters
            1. provider: Instructs where to start the walk
            2. visitor: Object previously defined
            3. maxDepth: Tells us the max subdirectories we can walk through
    * Once both objects have been declared and defined, we can call `walker.walk` to walk through our
      directories given the start path and specified pattern. This will populate `files` within the `visitor` object with the path of all visited files.
    * In order to make sure that we properly walked through the requested directories we finish up by comparing the returned file
      paths to a couple of expected results using assertEquals. If the assertEquals pass then we know that our walk
      was done successfully.
    * We repeat the process of walking and using assertEquals for a number of different
      patterns and conditions.
    <!-- end of the list -->
  <br>

  For more clarification on datacat search synatx please visit: <br>[https://confluence.slac.stanford.edu/pages/viewpage.action?spaceKey=SRSPDC&title=Searching]
  <br>

## **SRS DataCatalog RESTful Interfaces**

### **FormParamConverterTest**

* **void testFlatDataset();**\
  Ensures that we can successfully populate a flatDataset using a dataset obtained through the  use of `FormParamConverter`
  on a hashmap.

  The test confirms the ability to populate a flatDataset by doing the following
    * Declare and define the variable (dsMap)
        * Type: HashMap
        * Value: Null
    * Declare and define the variable (b)
        * Type: Dataset.builder
        * Null
    * Declare amd define the variable (ds)
        * Type: FlatDataset
        * Value: Null
    * Set (dsMap) equal to startDataset("hello");
        * This will make (dsMap) a hashmap with its value (name) set to "hello"
    * Set (b) equal to FormParamConverter.getDatasetBuilder( dsMap );
        * Through this, dsMap (hashmap) is being converted to a dataset then
          (b) is being set equal to it.
    * Set (ds) equal to b while typecasting (FlatDataset) and using a builder to efficiently populate the variable
      (ds) using the predefined fields of a dataset.
    * (ds) Should now be populated with the fields originally specified in (dsMap)
    * To confirm this, the contents of (ds) are compared to a variety of expectedResults using assetEqual test cases.
    * If all the cases pass then we know that we are properly able to populate a flatDataset through the conversion
      of a hashmap.
      
### **RequestViewTest**

* **void testFolder();**\
  Ensures that we can properly request views by creating an instance/object of the class `RequestView` given specific
  parameters.

  The test confirms the ability to request views via an instance of `RequestView` by doing the following
    * Declares a variable called (rv)
        * Type: RequestView
    * Declares a variable called (mvmap) and sets it equal to the result
      of `UriComponent.decodeMatrix( "/path; children", true);`
        * `UriComponent.decodeMatrix` takes a provided pathSegment, parses it, and returns it to be stored inside the
          Map `mvmap` as a key.
    * Using the now populated (mvmap), the variable (rv) is defined as an object with its parameters being
      `RecordType.FOLDER and mvmap`
    * With rv now containing our requested view we confirm its contents by testing it against a multitude of expected
      outputs.
        * This is done by using assertTrue(rv.containsKey("KEY")) and checking to see if it contains the expected keys.

### **ContainerResourceTest**

* **void testPatchJson();**\
  Tests to see if we can successfully update a resource using the HTTP method PATCH.
  
  Test Overview:
    * Using `DatasetsResourceTest.generateFoldersAndDatasetsAndVersions(this, 2, 2);` create test folders and datasets.
    * Create a request to the server using the HTTP method PATCH.
        * `Target` specifies the resource we wish to update.
        * `Header` contains carries credentials containing the authentication information of the client for the
          resource being requested.
        * `Method` contains the HTTP method we wish to use in our opperation. In this case we are using PATCH (same as 
          PUT).
        * `Entity` contains the JSON we will be updating the resource with.
    * Check the HTTP status code to make sure that the opperation was successfully carried out. 
        * HTTP status code 200 is expected.
  <br>
          
* **void testDeleteMetadata();**
  Tests to see if we can successfully delete the Metadata field from within a resource using the HTTP method PATCH.
  
  Test Overview:
    * Using `DatasetsResourceTest.generateFoldersAndDatasetsAndVersions(this, 2, 2);` create test folders and datasets.
      <br><br>
    * Create a request to the server using the HTTP method PATCH and target path ("/folders.txt/testpath/folder00001").
    * Check the HTTP status code to make sure that the operation was successfully carried out.
        * HTTP status code 200 is expected.
    * Check to make sure that the MetaData field is present and has a value.
        * This is confirmed through the use of assertTrue and `.contains`
      <br><br>
    * Create a request to the server using the HTTP method GET and target path ("/folders.json/testpath/folder00001").
    * Check the HTTP status code to make sure that the operation was successfully carried out.
        * HTTP status code 200 is expected.
    * Check to make sure that the MetaData field is present and has a value.
        * This is confirmed through the use of assertTrue and `.contains`
      <br><br>
    * Create a request to the server using the HTTP method PATCH and target path ("/folders.txt/testpath/folder00001").
        * `Entity` contains an identical JSON whose only difference is that the metadata field data has been set to null.
    * Check the HTTP status code to make sure that the operation was successfully carried out.
        * HTTP status code 200 is expected.
    * Check to make sure that the MetaData field is present and now has a NULL value.
        * This is confirmed through the use of assertFalse and `.contains`
      <br>
          
* **void testCreate();**
* **void testCreateJson();**
* **void testDeleteFolders();**
* **void testDeletePopulatedFolders();**
* **void testPatchJson();**




### **DatasetsResourceTest**
* **void testCreateDatasetsAndViews();**
* **void testCreation();**
* **void testCreationJson();**
* **void testPatchJson();**
* **void testCreationTwice();**
* **void testMergeCreation();**


### **PathResourceTest**
* **void testGetRootChildren();**
* **void testGetChildren();**
* **void testGetChildContainers();**
* **void testHead();**


### **PermissionsResourceTest**
* **void testQuery();**


### **SearchResourceTest**

* **void testBasicSearch();**

## **SRS DataCatalog Client**

### **DatasetClientsTest**

* **void testCreation();**\
  This test ensures that we are able to create `Client`, generate `Folders` and create `Dataset`.

  `Client` creation is tested by calling `getDatacatClient()` method, in which it declares `URL` and `filter`. It then
  uses these variables to build `Client` object by calling `.build()`.

  `Folder` creation is tested by calling static method `generateFolders(Client testClient, int folders)`
  from `ContainerClientTest` class. This method takes in `Client` object and `int` that specifies the number of folders
  you want to create and eventually calls `.createContainer()` which makes a POST request to the back-end and
  create `Folder`.

  `Dataset` creation is tested by calling `createOne(Client client)` method. This method eventually
  returns `DatasetModel` object by calling `.createDataset()` which makes a POST request to the back-end to
  create `Dataset`.


* **void testDatasetAcl();**\
  This test is about permissions. After generating test folder and test dataset,
  it tests their permissions in two parts:

    1. The test ensures that we can get the ACL of a dataset at a given path by making GET request to the back-end, and
       the returned first entry contains string `SRS`.
    2. The test ensures you can check permissions for you or a specific ACL user group. If group is not specified, the
       expected permissions are READ, INSERT, WRITE, DELETE and ADMIN. If group is specified, then return permissions as
       seen by this group. In this test case, `fake@` is used as group name and the expected permission is READ.

## Python Client Test Instruction

Once you have completed all of the [Prerequisites](INSTALL.md/#python-client-test) for Python Client Test, you can begin
testing by navigating to the top level of python client ``/slaclab-datacat/client/python`` where ``setup.py`` is located
and run the following command line:

```
$ pip3 install .
```

This will help Python to find packages that are imported in the tests.

Now you can run ``pytest`` at the top level to run Python client tests.

## Python Client Tests

### **ConfigTest**

* **test_hmac_config_file()**\
  Test that ensures the authentication strategy type is HMACAuth.



* **test_srs_config_file()**\
  Test that ensures the authentication strategy type is HMACAuthSRS.

* **test_srs_config_file_none()**\
  Test that ensures if authentication strategy type is empty, we should not receive authentication object.


* **test_srs_no_config()**\
  Test that ensures if configuration is not set, we use default URL for configuration URL and authentication object should not be received. Furthermore, in such circumstance we should not receive client object to interact with Datacat either.



* **test_srs_no_config_with_mode()**\
  Test that ensures if configuration is not set **and** mode is set to ``prod``(production) we use default production URL for configuration URL and authentication object should not be received. Furthermore, in such circumstance we should not receive client object to interact with Datacat either.



### **ModelTest**

* **test_unpack()**\
  Test to ensure we can convert JSON back to python object.

  In this test, ``Dataset``, ``Group``, and ``FullDataset`` python objects(dict) are created and converted to JSON by
  calling ``pack()`` and are converted back to python objects(dict) by calling ``unpack()``.


* **test_pack()**\
  Test to ensure we can convert python object to JSON.

  In this specific test case, ``Dataset`` object is constructed as python object, and ``pack()`` is called to convert it
  into JSON. Eventually the ``Dataset`` JSON string get printed.

* **test_build_dataset()**\
  Test to ensure the equality of building a dataset in two ways.

  In this specific test case, the first way of building ``dataset`` is to call ``build_dataset``. This function, other
  than taking in all the arguments, also performs extra operations on arguments. Eventually, it constructs ``params``
  dictionary, passing it to construct ``dataset`` object and return the object to caller.

  The second way of building ``dataset`` is to call ``Dataset`` constructor directly. The test ensures that these two
  methods are the same by serializing both of these object and calling ``assertEqual``.
