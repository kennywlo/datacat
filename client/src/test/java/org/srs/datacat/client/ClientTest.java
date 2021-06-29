package org.srs.datacat.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.commons.configuration.ConfigurationException;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.srs.datacat.client.exception.DcClientException;
import org.srs.datacat.client.exception.DcException;
import org.srs.datacat.model.DatacatNode;
import org.srs.datacat.model.DatasetModel;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.model.ModelProvider;
import org.srs.datacat.rest.App;
import org.srs.datacat.rest.FormParamConverter;
import org.srs.datacat.rest.JacksonFeature;
import org.srs.datacat.rest.resources.ContainerResource;
import org.srs.datacat.rest.resources.DatasetsResource;
import org.srs.datacat.rest.resources.PathResource;
import org.srs.datacat.rest.resources.PermissionsResource;
import org.srs.datacat.shared.Dataset;
import org.srs.datacat.shared.Provider;
import org.srs.datacat.shared.metadata.MetadataEntry;
import org.srs.datacat.test.DbHarness;
import org.srs.datacat.test.HSqlDbHarness;
import org.srs.datacat.vfs.TestUtils;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.*;

/**
 *
 * @author bvan
 */
public class ClientTest extends JerseyTest {


    // TODO: Integrate in with a webserver instance
    private final Provider provider = new Provider();
    static final ObjectMapper mdMapper = new ObjectMapper();

    @Override
    protected Application configure(){
        try {
            DbHarness harness = DbHarness.getDbHarness();

            ResourceConfig app = new App(harness.getDataSource(), provider, TestUtils.getLookupService())
                    .register(DatasetClientsTest.TestSecurityFilter.class)
                    .register(ContainerResource.class)
                    .register(PermissionsResource.class)
                    .register(PathResource.class)
                    .register(DatasetsResource.class);
            app.property(ServerProperties.TRACING, "ALL");
            return app;
        } catch(SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void testMain() throws Exception{

        Client c = new Client(new URI("http://lsst-db2:8180/datacat-v0.3/r"));
        DatacatNode n = c.getContainer("/LSST", "dataset");
        n = c.getContainer("/LSST", "basic");
        n = c.getObject("/LSST");

        System.out.println(n.toString());
        List<? extends DatacatNode> children = c.getChildren("/LSST", "current", "master");
        for(DatacatNode child: children){
            System.out.println(child.toString());
        }
        children = c.searchForDatasets("/LSST", "current", "master", "", null, null, 0, 1000).getResults();
        for(DatacatNode child: children){
            System.out.println(child.toString());
            System.out.println(child.getClass());
        }

        HashMap<String, Object> md = new HashMap<>();
        md.put("someFloat4", new Double(43211));
        DatasetModel ds = new Provider().getDatasetBuilder().versionMetadata(md).build();
        try{
            c.patchDataset("/LSST/dataset0001", null, "SLAC", ds);
        } catch (DcClientException e){
            System.out.println(e.toString());
        } catch (DcException ex){
            System.out.println(ex.toString());
        }

    }

    public void testX() throws IOException, ConfigurationException, URISyntaxException{
        Map<String,String> config = Config.defaultConfig("0.3-dev");

        Client c = ClientBuilder.newBuilder(config)
                .addClientRequestFilter(new LoggingFilter()).build();
        DatacatNode n = c.getContainer("/LSST", "dataset");
        n = c.getContainer("/LSST", "basic");
        n = c.getObject("/LSST");

        System.out.println(n.toString());
        List<? extends DatacatNode> children = c.getChildren("/LSST", "current", "master");
        for(DatacatNode child: children){
            System.out.println(child.toString());
        }
        children = c.searchForDatasets("/LSST", "current", "master", "", null, null, 0, 1000).getResults();
        for(DatacatNode child: children){
            System.out.println(child.toString());
            System.out.println(child.getClass());
        }
    }

    String fileFormat;
    String dataType;
    String logicalFolderPath;
    String site;
    String location;
    Map<String, Object> versionMetadata = new HashMap<>();

    public void testCreateDataset() throws IOException, URISyntaxException, ConfigurationException{


        ModelProvider mp = new Provider();

        Map<String, String> config = Config.defaultConfig();
        Client c = ClientBuilder.newBuilder(config)
                .addClientRequestFilter(new LoggingFilter()).build();

        DatasetModel newDataset = mp.getDatasetBuilder()
                .name("dataset0001")
                .fileFormat(fileFormat)
                .dataType(dataType)
                .site(site)
                .resource(location)
                .versionMetadata(versionMetadata)
                .build();
        c.createDataset(logicalFolderPath, newDataset);
    }


    @Test
    public void testCreateDependency() throws IOException, URISyntaxException{
        /**
         * Ensures we could create dependency from Client side
         * @author ChufanWu
         */
        String parent = "/testpath/folder00000";

        //Create folder
        DatasetClientsTest dsct = new DatasetClientsTest();
        Client client = dsct.getDatacatClient();
        ContainerClientTest.generateFolders(client,1);

        //create Dataset1
        String name1 = "dataset0001";
        Response resp1 = createOne(parent, name1);
        TestCase.assertEquals(201,resp1.getStatus());

        //getting versionPk of dataset1
        String r = resp1.readEntity(String.class);
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, String> result = mapper.readValue(r, HashMap.class);
        String ds1VersionPk = String.valueOf(result.get("versionPk"));

        //create Dataset2 with dependency
        String name2 = "dataset0002";
        Response resp2 = createOne(parent, name2, ds1VersionPk);
        TestCase.assertEquals(201,resp2.getStatus());
    }

    /**
     * Create dataset without dependency metadata
     * @author Chufan Wu
     */
    public Response createOne(String parent, String name) throws JsonProcessingException {

        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap<>();
        entity.add("name", name);
        entity.add("dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add("datasetSource", HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
        entity.add("fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add("versionId", Integer.toString(DatasetView.NEW_VER));
        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        entity.add("versionMetadata", mdMapper.writeValueAsString(MetadataEntry.toList(metadata)));
        Response resp = target("/datasets.json" + parent)
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form(entity));
        Response resp2 = target("/datasets.json/testpath/folder00000/" + name)
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();

        String r = resp2.readEntity(String.class);
        System.out.println(r);

        return resp;
    }


    /**
     * Create dataset with dependency metadata
     * @author Chufan Wu
     */
    public Response createOne(String parent, String name, String versionPk) throws JsonProcessingException, IOException {

        MultivaluedHashMap<String, String> entity = new MultivaluedHashMap<>();
        entity.add("name", name);
        entity.add("dataType", HSqlDbHarness.JUNIT_DATASET_DATATYPE);
        entity.add("datasetSource", HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
        entity.add("fileFormat", HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
        entity.add("versionId", Integer.toString(DatasetView.NEW_VER));
        HashMap<String, Object> metadata = ContainerClientTest.generateDependencies(versionPk);
        entity.add("versionMetadata", mdMapper.writeValueAsString(MetadataEntry.toList(metadata)));
        Response resp = target("/datasets.json" + parent)
                .property(ClientProperties.FOLLOW_REDIRECTS, "false")
                .request()
                .header("authentication", DbHarness.TEST_USER)
                .post(Entity.form(entity));
        Response resp2 = target("/datasets.json/testpath/folder00000/" + name)
                .request(MediaType.APPLICATION_JSON)
                .header("authentication", DbHarness.TEST_USER)
                .get();

        String r = resp2.readEntity(String.class);
        System.out.println(r);

        return resp;
    }



}
