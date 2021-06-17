


package org.srs.datacat.rest.resources;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import javax.sql.DataSource;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.srs.datacat.dao.DAOFactory;
import org.srs.datacat.dao.DAOTestUtils;
import org.srs.datacat.dao.DatasetDAO;
import org.srs.datacat.dao.sql.mysql.DAOFactoryMySQL;
import org.srs.datacat.dao.sql.search.DatasetSearch;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.model.RecordType;
import org.srs.datacat.rest.App;
import org.srs.datacat.rest.FormParamConverter;
import org.srs.datacat.rest.JacksonFeature;
import org.srs.datacat.shared.Dataset;
import org.srs.datacat.shared.FullDataset;
import org.srs.datacat.shared.Provider;
import org.srs.datacat.shared.metadata.MetadataEntry;
import org.srs.datacat.test.DbHarness;
import org.srs.datacat.test.HSqlDbHarness;
import org.srs.datacat.vfs.TestUtils;
import org.srs.vfs.PathUtils;

/**
 *
 * @author bvan
 */
public class ContainerResourceTest extends JerseyTest {

    static DbHarness harness;
    static DataSource ds = null;
    static DAOFactory factory;
    DatasetSearch datacatSearch;

    private final Provider modelProvider = new Provider();
    static final ObjectMapper mdMapper = new ObjectMapper();

    public ContainerResourceTest(){ }



    public static void generateFolders(JerseyTest testInstance, int folders) throws IOException{
        String parent = "/testpath";
        // Create 10 folders
        int expectedStatus = 201;
        for(int i = 0; i < folders; i++){
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            System.out.println("/folders.txt" + newPath);
            Response resp = testInstance.target("/folders.txt" + parent)
                    .request()
                    .header("authentication", DbHarness.TEST_USER)
                    .post( Entity.form(new Form("name",name)));
            TestCase.assertEquals(resp.readEntity(String.class), expectedStatus, resp.getStatus());
        }
    }

    public static void deleteFolders(JerseyTest testInstance, int folders) throws IOException{
        String parent = "/testpath";

        Response resp;
        for(int i=0; i < folders; i++) {
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            System.out.println("/folders.txt" + newPath);
            resp = testInstance.target("/folders.txt" + parent)
                    .request()
                    .header("authentication", DbHarness.TEST_USER)
                    .delete();
            System.out.println(resp.getStatus());
        }
    }

    @Override
    protected Application configure(){
        DbHarness harness = null;
        try {
            harness = DbHarness.getDbHarness();
        } catch(SQLException ex) {
            System.out.println(ex);

        }

        ResourceConfig app = new App(harness.getDataSource(), modelProvider, TestUtils.getLookupService())
                .register(TestSecurityFilter.class)
                .register(ContainerResource.class)
                .register(DatasetsResource.class)
                .register(PathResource.class);
        app.property( ServerProperties.TRACING, "ALL");
        for(Resource r: app.getResources()){
            System.out.println(r.getPath());
        }
        return app;

    }


    @BeforeClass
    public static void setUpDb() throws SQLException, IOException{
        harness = DbHarness.getDbHarness();
        harness.getDataSource();
        ds = harness.getDataSource();
        factory = new DAOFactoryMySQL(ds);
    }


    @Test
    public void testDispatch(){
        Response resp;

        resp = target("/folders.txt/testpath")
                .request()
                .get();
        System.out.println(resp.readEntity(String.class));

        // setup more complicated test
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", "dispatchTest");

        resp = target("/folders.txt/testpath")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form( entity ));
        TestCase.assertEquals( Status.CREATED, Status.fromStatusCode(resp.getStatus()));

        entity = new MultivaluedHashMap<>();
        entity.add( "name", "dispatchTest2");
        resp = target("/folders.txt/testpath/dispatchTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form(entity));
        TestCase.assertEquals( Status.CREATED, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/dispatchTest/dispatchTest2")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .get();
        TestCase.assertEquals( Status.OK, Status.fromStatusCode(resp.getStatus()));

        // We have a structure we can test with now       
        resp = target("/folders.txt/testpath/dispatchTest%2fdispatchTest2%3bv=2")
                .request()
                .get();
        TestCase.assertEquals("Escaped semicolon should fail", Status.NOT_FOUND, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/dispatchTest%2fdispatchTest2;v=0")
                .request()
                .get();
        TestCase.assertEquals("Should have parsed request", Status.OK, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt%2ftestpath%2fdispatchTest%3bdispatchTest2")
                .request()
                .get();
        TestCase.assertEquals("Should have failed to parse resource", Status.NOT_FOUND, Status.fromStatusCode(resp.getStatus()));
    }

    @Test
    public void testCreate() throws IOException{
        Response resp;
        System.out.println("getting /folders.txt/testpath");
        resp = target("/folders.txt/testpath")
                .request()
                .get();
        System.out.println("got:" + resp.readEntity( String.class));

        generateFolders(this, 10);
        int expectedStatus = 200;
        String parent = "/testpath";
        for(int i = 0; i < 10; i++){
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            resp = target("/folders.txt" + newPath)
                    .request( MediaType.APPLICATION_JSON )
                    .get();
            TestCase.assertEquals(expectedStatus, resp.getStatus());
        }

        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", "createFolderTest");

        resp = target("/folders.txt/testpath")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form( entity ));
        TestCase.assertEquals( Status.CREATED, Status.fromStatusCode(resp.getStatus()));

        System.out.println("getting /folders.txt/testpath/createFolderTest");
        resp = target("/folders.txt/testpath/createFolderTest")
                .request()
                .get();
        System.out.println(resp.readEntity( String.class));

        resp = target("/folders.txt/testpath/createFolderTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form( entity ));
        TestCase.assertEquals( Status.CREATED, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/createFolderTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals( Status.CONFLICT, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/createFolderTest/createFolderTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals( Status.NO_CONTENT, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/createFolderTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals( Status.NO_CONTENT, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.txt/testpath/createFolderTest")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals( Status.NOT_FOUND, Status.fromStatusCode(resp.getStatus()));
        
        /*resp = target("/folders.txt/junit/createFolderTest")
            .request()
            .delete();
        TestCase.assertEquals( Status.NO_CONTENT, Status.fromStatusCode(resp.getStatus()));
                */

        deleteFolders(this, 10);
    }

    @Test
    public void testCreateJson() throws IOException {
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", "dispatchTest");
        DatasetContainer fold1 =
                FormParamConverter.getContainerBuilder(RecordType.FOLDER, entity).build();

        Response resp = target("/folders.txt/testpath")
                .request()
                .get();

        TestCase.assertEquals(Status.OK, Status.fromStatusCode(resp.getStatus()));

        resp = target("/folders.json/testpath")
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(fold1, MediaType.APPLICATION_JSON));

        TestCase.assertEquals( Status.CREATED, Status.fromStatusCode(resp.getStatus()));
    }

    @Test
    public void testDeleteFolders() throws IOException{
        generateFolders(this, 10);
        Response resp;
        String parent = "/testpath";

        int expectedStatus = 200;
        for(int i = 0; i < 10; i++){
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            resp = target("/folders.txt" + newPath)
                    .request( MediaType.APPLICATION_JSON )
                    .get();
            TestCase.assertEquals(expectedStatus, resp.getStatus());
        }

        expectedStatus = 204;
        for(int i = 0; i < 10; i++){
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            resp = target("/folders.txt" + newPath)
                    .request()
                    .header("authentication", DbHarness.TEST_USER)
                    .delete();
            TestCase.assertEquals(expectedStatus, resp.getStatus());
        }

        expectedStatus = 404;
        for(int i = 0; i < 10; i++){
            String name =String.format("folder%05d", i);
            String newPath = PathUtils.resolve(parent, name);
            resp = target("/folders.txt" + newPath)
                    .request( MediaType.APPLICATION_JSON )
                    .get();
            TestCase.assertEquals(resp.getStatus(), expectedStatus);
        }

    }

    @Test
    public void testDeletePopulatedFolders() throws IOException{
        DatasetsResourceTest.generateFoldersAndDatasetsAndVersions(this, 5, 5);
        Response resp = target("/folders.txt/testpath")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals(409, resp.getStatus());

        resp = target("/folders.txt/testpath/folder00001")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();
        TestCase.assertEquals(409, resp.getStatus());
        DatasetsResourceTest.deleteFoldersAndDatasetsAndVersions(this, 5, 5);
    }

    @Test
    public void testPatchJson() throws IOException{
        DatasetsResourceTest.generateFoldersAndDatasetsAndVersions(this, 2, 2);
        Response resp = target("/folders.txt/testpath/folder00001")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .method("PATCH",
                        Entity.entity("{\"_type\":\"folder\", \"description\":\"A folder00001\", \"metadata\":[{\"mdKey\":\"mdValue\"}]}",
                                MediaType.APPLICATION_JSON));
        TestCase.assertEquals(200, resp.getStatus());
        DatasetsResourceTest.deleteFoldersAndDatasetsAndVersions(this, 2, 2);
    }

    @Test
    public void testDeleteMetadata() throws IOException{
        DatasetsResourceTest.generateFoldersAndDatasetsAndVersions(this, 2, 2);
        Response resp = target("/folders.txt/testpath/folder00001")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .method("PATCH",
                        Entity.entity("{\"_type\":\"folder\", \"description\":\"A folder00001\", \"metadata\":[{\"mdKey\":\"mdValue\"}]}",
                                MediaType.APPLICATION_JSON));

        TestCase.assertEquals(200, resp.getStatus());
        TestCase.assertTrue(resp.readEntity(String.class).contains("mdKey"));

        resp = target("/folders.json/testpath/folder00001")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .get();

        TestCase.assertEquals(200, resp.getStatus());
        TestCase.assertTrue(resp.readEntity(String.class).contains("mdKey"));

        resp = target("/folders.txt/testpath/folder00001")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .method("PATCH",
                        Entity.entity("{\"_type\":\"folder\", \"description\":\"A folder00001\", \"metadata\":[{\"mdKey\":null}]}",
                                MediaType.APPLICATION_JSON));

        TestCase.assertEquals(200, resp.getStatus());
        TestCase.assertFalse(resp.readEntity(String.class).contains("mdKey"));

        resp = target("/folders.json/testpath/folder00001")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .get();

        TestCase.assertEquals(200, resp.getStatus());
        TestCase.assertFalse(resp.readEntity(String.class).contains("mdKey"));

        DatasetsResourceTest.deleteFoldersAndDatasetsAndVersions(this, 2, 2);
    }


    @Test
    public void testDeleteDependencies() throws IOException{
        /**
         * @author CesarAxel
         */
        /**
         * In this test we are ensuring that we can properly generate folders/datasets, attach an appropriate
         * dependency to a dataset, and then delete said dependency through a cascading deletion of the dataset itself.
         */


//        String for storing the response entity
        String responseEntityString;

//        Generates a Folder
        generateFolders(this, 1);

//        Creates dataset0000
        String parent = "/testpath/folder00000";
        String name = "dataset0000";
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
        Dataset req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request POST for dataset0000
        Response resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());

//        The following retrieves the versionPK of dataset0000 and stores it in a variable for later use
//          Sends the HTTP method request GET for dataset0000
        Response resp2 = target("/datasets.json/testpath/folder00000/dataset0000")
                .request( MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp2.readEntity(String.class);

//          Declare a new ObjectMapper for easy access to EntityString fields
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, String> result = mapper.readValue(responseEntityString, HashMap.class);

//          Print and save the field value (VersionPK) for later use in dependency testing
        System.out.println("\nDataset0000 VersionPK");
        System.out.println(String.valueOf(result.get("versionPk")));
        String dataset0000PK = String.valueOf(result.get("versionPk"));


//        Creates dataset0001
        parent = "/testpath/folder00000";
        name = "dataset0001";
        entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
        req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request POST for dataset0001
        resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());

//        The following retrieves the versionPK of dataset0001 and stores it in a variable for later use
//          Sends the HTTP method request GET for dataset0001
        resp2 = target("/datasets.json/testpath/folder00000/dataset0001")
                .request( MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp2.readEntity(String.class);

//          Declare a new ObjectMapper for easy access to EntityString fields
        mapper = new ObjectMapper();
        result = mapper.readValue(responseEntityString, HashMap.class);

//          Print and save the field value (VersionPK) for later use in dependency testing
        System.out.println("\nDataset0001 VersionPK");
        System.out.println(String.valueOf(result.get("versionPk")));
        String dataset0001PK = String.valueOf(result.get("versionPk"));

//        Combines the datasetPKs into a single, comma delimited, string
        String combinedDependents = dataset0000PK + "," + dataset0001PK;

//        Creates dataset0002 (with dependency)
        name = "dataset0002";
        entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        metadata = DAOTestUtils.generateDependencies("Test",combinedDependents,"predecessor", metadata);
        entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
        req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request for dataset0002
        resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());
        


//        Print datasets within folder0000 to console (before deletion)
        resp = target("/folders.txt/testpath/folder00000;children")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp.readEntity(String.class);

        System.out.println("\nFolder0000 (before dataset0001 deletion)");
        System.out.println(responseEntityString);


//        Deletes dataset0002
        name = String.format("/dataset0002");
        String child = parent + name;
        resp = target("/datasets.txt" + child)
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .delete();


//        Prints datasets within folder0000 to console (after deletion)
        resp = target("/folders.txt/testpath/folder00000;children")
                .request( MediaType.APPLICATION_JSON )
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp.readEntity(String.class);

        System.out.println("Folder0000 (after dataset0002 deletion)");
        System.out.println(responseEntityString);



        /*
        - In order confirm the existence and deletion of a dependency we will need to check the contents of the table DatasetDependency
        - To to this we will need to use the Evaluate Expression module.
        - For this to work however you will have to make sure you set a breakpoint at a spot where a connection to the database is possible.
        - This needs to be performed two times, once to make sure that the table contents are properly uploaded and once to make sure they are properly deleted.
        */

        /*
        Copy and paste the code below into the Evaluate Expression module and the results will be printed to the console.
        -----------------------------------------------------------------------------------------------------------
        getConnection().createStatement().executeQuery("select Name from DatasetDependency");
        ResultSet rs = getConnection().createStatement().executeQuery("select * from DatasetDependency");
        System.out.println("\ndataset_dependency_table:");
        System.out.println("************************\n");

        while (rs.next()) {

            String nameOut = rs.getString("Name");
            System.out.println("Name:");
            System.out.println(nameOut);

            String dependencyOut = rs.getString("Dependency");
            System.out.println("Dependency:");
            System.out.println(dependencyOut);

            String dependentOut = rs.getString("Dependent");
            System.out.println("Dependent:");
            System.out.println(dependentOut);

            String dependentTypeOut = rs.getString("DependentType");
            System.out.println("DependentType:");
            System.out.println(dependentTypeOut);
            System.out.println("\n");
        }
        -----------------------------------------------------------------------------------------------------------
        */
    }


    @Test
    public void testGetDependencies() throws IOException {
        /**
         * @author CesarAxel
         */
        /**
         * In this test we are ensuring that we can properly generate folders/datasets, attach an appropriate
         * dependency to a dataset, and then print out the dependency information for the dataset with dependency
         * information.
         */


//        String for storing the response entity
        String responseEntityString;

//        Generates a Folder
        generateFolders(this, 1);

//        Creates dataset0000
        String parent = "/testpath/folder00000";
        String name = "dataset0000";
        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap<>();
        entity.add("name", name);
        entity.add("dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add("fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add("versionId", Integer.toString(DatasetView.NEW_VER));
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        entity.add("versionMetadata", mdMapper.writeValueAsString(MetadataEntry.toList(metadata)));
        Dataset req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request POST for dataset0000
        Response resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());

//        The following retrieves the versionPK of dataset0000 and stores it in a variable for later use
//          Sends the HTTP method request GET for dataset0000
        Response resp2 = target("/datasets.json/testpath/folder00000/dataset0000")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp2.readEntity(String.class);

//          Declare a new ObjectMapper for easy access to EntityString fields
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, String> result = mapper.readValue(responseEntityString, HashMap.class);

//        Save the field value (VersionPK) for dataset0000
        String dataset0000PK = String.valueOf(result.get("versionPk"));

//        Creates dataset0001
        parent = "/testpath/folder00000";
        name = "dataset0001";
        entity = new MultivaluedHashMap<>();
        entity.add("name", name);
        entity.add("dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add("fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add("versionId", Integer.toString(DatasetView.NEW_VER));
        metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        entity.add("versionMetadata", mdMapper.writeValueAsString(MetadataEntry.toList(metadata)));
        req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request POST for dataset0001
        resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());

//        The following retrieves the versionPK of dataset0001 and stores it in a variable for later use
//          Sends the HTTP method request GET for dataset0001
        resp2 = target("/datasets.json/testpath/folder00000/dataset0001")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp2.readEntity(String.class);

//          Declare a new ObjectMapper for easy access to EntityString fields
        mapper = new ObjectMapper();
        result = mapper.readValue(responseEntityString, HashMap.class);

//        Save the field value (VersionPK) for dataset0001
        String dataset0001PK = String.valueOf(result.get("versionPk"));

//        Combines the datasetPKs into a single, comma delimited, string
        String combinedDependents = dataset0000PK + "," + dataset0001PK;

//        Creates dataset0002 (with dependency)
        name = "dataset0002";
        entity = new MultivaluedHashMap<>();
        entity.add("name", name);
        entity.add("dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add("fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add("versionId", Integer.toString(DatasetView.NEW_VER));
        metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        metadata = DAOTestUtils.generateDependencies("Test", combinedDependents, "predecessor", metadata);
        entity.add("versionMetadata", mdMapper.writeValueAsString(MetadataEntry.toList(metadata)));
        req = FormParamConverter.getDatasetBuilder(entity).build();

//        Sends the HTTP method request for dataset0002
        resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        TestCase.assertEquals(201, resp.getStatus());

//        get versionPK for dataset0002
        resp2 = target("/datasets.json/testpath/folder00000/dataset0002")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        responseEntityString = resp2.readEntity(String.class);

        mapper = new ObjectMapper();
        result = mapper.readValue(responseEntityString, HashMap.class);

//        Save the field value (VersionPK) for dataset0002
        long dataset0002PK = Long.parseLong((String.valueOf(result.get("versionPk"))));

        DatasetContainer depInfo = DAOTestUtils.getDependencies(factory, dataset0002PK,"/testpath/folder00000/dataset0002","successor");
        depInfo.getMetadataMap().get("dependents");

//        Tests to make sure that the proper dependents were returned by getDependents.

        String dependentsReturn = (depInfo.getMetadataMap().get("dependents")).toString();

        TestCase.assertTrue(dependentsReturn.contains(dataset0000PK));
        TestCase.assertTrue(dependentsReturn.contains(dataset0001PK));
    }
}
