/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.srs.datacat.rest.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import junit.framework.TestCase;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.rest.App;
import org.srs.datacat.rest.FormParamConverter;
import org.srs.datacat.rest.JacksonFeature;
import org.srs.datacat.shared.Dataset;
import org.srs.datacat.shared.Provider;
import org.srs.datacat.test.DbHarness;
import org.srs.datacat.test.HSqlDbHarness;
import org.srs.datacat.vfs.TestUtils;
import org.srs.datacat.shared.metadata.MetadataEntry;
import org.srs.vfs.PathUtils;

/**
 *
 * @author bvan
 */
public class DatasetsResourceTest extends JerseyTest {
    private final Provider modelProvider = new Provider();
    
    static final ObjectMapper mdMapper = new ObjectMapper();

    static {
        AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
        AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        AnnotationIntrospector pair = new AnnotationIntrospectorPair(primary, secondary);
        mdMapper.setAnnotationIntrospector( pair );
    }
    
    public DatasetsResourceTest(){
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
                .register(PathResource.class)
                .register(DatasetsResource.class);
        app.property( ServerProperties.TRACING, "ALL");
        for(Resource r: app.getResources()){
            System.out.println(r.getPath());
        }
        return app;
    }


    @Test
    public void testCreateDatasetsAndViews() throws IOException{
        generateFoldersAndDatasetsAndVersions(this, 10, 100);
    }
       
    public Response createOne() throws JsonProcessingException{
        String parent = "/testpath/folder00000";
        String name = "dataset0001";
        
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "datasetSource", HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);

        entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
        Response resp = target("/datasets.json" + parent)
                .property( ClientProperties.FOLLOW_REDIRECTS, "false")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form(entity));
        
        return resp;
    }
    
    public Response createOneWithLocation(String site, String resource) throws JsonProcessingException{
        String parent = "/testpath/folder00000";
        String name = "dataset0001";
        
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "datasetSource", HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);

        entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
        entity.add("site", site);
        entity.add("resource", resource);
        
        Response resp = target("/datasets.json" + parent)
                .property( ClientProperties.FOLLOW_REDIRECTS, "false")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form(entity));
        
        
        return resp;
    }

    
    @Test
    public void testCreation() throws JsonProcessingException, IOException {
        ContainerResourceTest.generateFolders(this, 1);
        Response resp = createOne();
        TestCase.assertEquals(201, resp.getStatus());
    }
    
    @Test
    public void testCreationJson() throws JsonProcessingException, IOException {
        ContainerResourceTest.generateFolders(this, 1);
        String parent = "/testpath/folder00000";
        String name = "dataset0001";
        
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
        System.out.println(req.toString());
        
        Response resp = target("/datasets.json" + parent)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));

        printTrace( resp );
        TestCase.assertEquals(201, resp.getStatus());
        System.out.println(resp.readEntity(String.class));
    }
    
    @Test
    public void testPatchJson() throws JsonProcessingException, IOException {
        ContainerResourceTest.generateFolders(this, 1);
        String name = "dataset0001";
        String parent = "/testpath/folder00000";
        String ds = "/testpath/folder00000/dataset0001";
        
        MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
        entity.add( "name", name);
        entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
        entity.add("resource", "/nfs/fake/path");
        entity.add("site", "SLAC");

        Dataset req = FormParamConverter.getDatasetBuilder(entity).build();
        System.out.println(req.toString());
        
        Response resp = target("/datasets.json" + parent)
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        
        printTrace( resp );
        System.out.println(resp.readEntity(String.class));
        TestCase.assertEquals(201, resp.getStatus());
        
        resp = target("/datasets.json" + ds)
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();
        System.out.println(resp.readEntity(String.class));
        
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        metadata = new HashMap<>();
        metadata.put("patch", "example");
                
        req = new Dataset.Builder().checksum(Long.toHexString(12345L))
                .versionMetadata(metadata)
                .build();
        resp = target("/datasets.json" + ds)
            .register(new JacksonFeature(modelProvider))
            .property(ClientProperties.FOLLOW_REDIRECTS, "false")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON)
            .header("authentication", DbHarness.TEST_USER)
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON));
        System.out.println(resp.readEntity(String.class));
        TestCase.assertEquals(200, resp.getStatus());
        
    }
    
    /*@Test
    public void testCreateJson() throws JsonProcessingException, IOException {
        ContainerResourceTest.generateFolders(this, 1);
        String name = "dataset0001";
        String parent = "/testpath/folder00000";
        String ds = "/testpath/folder00000/dataset0001";
        
        String k_s = "\"%s\":\"%s\"";
        String k_n = "\"%s\":%f";
        StringBuilder json = new StringBuilder("{");
        json.append(String.format(k_s, "name", name))
            .append(String.format(k_s, "dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE)).append(",")
            .append(String.format(k_s, "fileFormat",  HSqlDbHarness.JUNIT_DATASET_FILEFORMAT)).append(",")
            .append(String.format(k_s, "versionId",  "new")).append(",")
            .append(String.format(k_s, "site",  "SLAC")).append(",")
            .append(String.format(k_s, "resource",  "/nfs/fake/path"))
            .append("}");

        Dataset req = FormParamConverter.getDatasetBuilder(entity).build();
        System.out.println(req.toString());
        
        Response resp = target("/datasets.json" + parent)
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", TestUtils.TEST_USER)
                .post(Entity.entity(req, MediaType.APPLICATION_JSON));
        
        printTrace( resp );
        System.out.println(resp.readEntity(String.class));
        TestCase.assertEquals(201, resp.getStatus());
        
        resp = target("/datasets.json" + ds)
                .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
                .register(new JacksonFeature(modelProvider))
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", TestUtils.TEST_USER)
                .get();
        System.out.println(resp.readEntity(String.class));
        
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put( numberName, numberMdValues[0]);
        metadata.put( alphaName, alphaMdValues[0]);
        metadata = new HashMap<>();
        metadata.put("patch", "example");
                
        req = new Dataset.Builder().checksum(12345L)
                .versionMetadata(metadata)
                .build();
        resp = target("/datasets.json" + ds)
            .register(new JacksonFeature(modelProvider))
            .property(ClientProperties.FOLLOW_REDIRECTS, "false")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON)
            .header("authentication", TestUtils.TEST_USER)
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON));
        System.out.println(resp.readEntity(String.class));
        TestCase.assertEquals(200, resp.getStatus());
        
    }*/

    
    @Test
    public void testCreationTwice() throws JsonProcessingException, IOException{
        ContainerResourceTest.generateFolders(this, 1);
        Response resp = createOne();
        TestCase.assertEquals(201, resp.getStatus());
        resp = createOne();
        printTrace(resp);
        TestCase.assertEquals(resp.readEntity(String.class), 409, resp.getStatus());
        
        resp.getLocation().getPath();
        resp = target(resp.getLocation().getPath())
            .property( ClientProperties.FOLLOW_REDIRECTS, "false")
            .request()
            .get();
        printTrace(resp);
        TestCase.assertEquals(200, resp.getStatus());
    }
    
    @Test
    public void testMergeCreation() throws JsonProcessingException, IOException{
        ContainerResourceTest.generateFolders(this, 1);

        Response resp = createOneWithLocation("SLAC","file://path/to/one.txt");
        TestCase.assertEquals(201, resp.getStatus());
        System.out.println(resp.readEntity(String.class));
        resp = createOneWithLocation("SLAC2","file://path/to/two.txt");
        
        TestCase.assertEquals(409, resp.getStatus());
        
        resp.getLocation().getPath();
        resp = target(resp.getLocation().getPath())
            .property( ClientProperties.FOLLOW_REDIRECTS, "false")
            .request()
            .get();
        printTrace(resp);
        System.out.println(resp.readEntity(String.class));
        TestCase.assertEquals(200, resp.getStatus());
    }
    
    private void printTrace(Response resp){
        ArrayList<String> headerNames = new ArrayList<>( resp.getHeaders().keySet() );
        Collections.sort( headerNames );
        for(String headerName: headerNames){
            System.out.println( headerName + ":" );
            for(Object trace: resp.getHeaders().get( headerName )){
                System.out.println( "\t" + trace );
            }
        }
    }
    
    public static void generateFoldersAndDatasetsAndVersions(JerseyTest testCase, int folderCount, int datasetCount) throws IOException{
        ContainerResourceTest.generateFolders(testCase, folderCount);
        
        for(int i = 0; i < folderCount; i++){
            String parent =PathUtils.resolve( "/testpath",String.format("folder%05d", i));
            for(int j = 0; j < datasetCount; j++){
                String name = String.format("dataset%05d", j);
                MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
                entity.add( "name", name);
                entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
                entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
                entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
                HashMap<String, Object> metadata = new HashMap<>();
                metadata.put(DbHarness.numberName, DbHarness.numberMdValues[i % 4]);
                metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[j % 4]);

                entity.add("versionMetadata",mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
                Response resp = testCase.target("/datasets.txt" + parent)
                    .request()
                    .header("authentication", DbHarness.TEST_USER)
                    .post(Entity.form(entity));
                if(resp.getStatus() == 409){
                    System.out.println("duplicate: datasets" + parent + "/" + name);
                    System.out.println(resp.readEntity(String.class));
                } else {
                    if(resp.getStatus() != 201){
                        System.out.println("Found error:");
                        System.out.println(resp.readEntity(String.class));
                    }
                    TestCase.assertEquals(201, resp.getStatus());
                }
            }
        }
    }

    public static void deleteFoldersAndDatasetsAndVersions(JerseyTest testCase, int folderCount, int datasetCount) throws IOException{
        for(int i = 0; i < folderCount; i++){
            String parent =PathUtils.resolve( "/testpath",String.format("folder%05d", i));
            for(int j = 0; j < datasetCount; j++) {
                String name = String.format("/dataset%05d", j);
                MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
                String child = parent + name;
                Response resp = testCase.target("/datasets.txt" + child)
                        .request()
                        .header("authentication", DbHarness.TEST_USER)
                        .delete();
                System.out.println(resp.getStatus());
            }
        }
        ContainerResourceTest.deleteFolders(testCase, folderCount);
    }

    /*
    public static void generateFoldersAndDatasetsAndVersionsAndLocations(JerseyTest testCase, int folderCount, int datasetCount) throws IOException{
        ContainerResourceTest.generateFolders(testCase, folderCount);

        // Create 20k datasets
        for(int i = 0; i < folderCount; i++){
            String parent =PathUtils.resolve( "/testpath",String.format("folder%05d", i));
            for(int j = 0; j < datasetCount; j++){
                String name = String.format("dataset%05d", j);
                MultivaluedHashMap<String,String> entity = new MultivaluedHashMap<>();
                entity.add( "name", name);
                entity.add( "dataType",HSqlDbHarness.JUNIT_DATASET_DATATYPE);
                entity.add( "datasetSource", HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
                entity.add( "fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
                entity.add( "versionId", Integer.toString(DatasetView.NEW_VER) );
                HashMap<String, Object> metadata = new HashMap<>();
                metadata.put( numberName, numberMdValues[i % 4]);
                metadata.put( alphaName, alphaMdValues[j % 4]);

                System.out.println(mdMapper.writeValueAsString(MetadataEntry.toList( metadata )));
                entity.add("versionMetadata",mdMapper.writeValueAsString(mdMapper.writeValueAsString(MetadataEntry.toList( metadata ))));
                Response resp = testCase.target("/datasets" + parent)
                    .request()
                    .post( Entity.form(entity));
                TestCase.assertEquals( "201",resp.getStatus());
            }
        }
    }
    */
    
    
}
