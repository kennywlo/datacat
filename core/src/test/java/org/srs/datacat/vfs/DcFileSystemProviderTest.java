 
package org.srs.datacat.vfs;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

import junit.framework.TestCase;
import org.junit.AfterClass;

import org.junit.BeforeClass;
import org.junit.Test;
import org.srs.datacat.dao.BaseDAO;
import org.srs.datacat.dao.ContainerDAO;
import org.srs.datacat.dao.DAOFactory;
import org.srs.datacat.dao.sql.mysql.DAOFactoryMySQL;
import org.srs.datacat.model.DatacatNode;

import org.srs.datacat.model.DatasetModel;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.DatacatRecord;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.model.ModelProvider;
import org.srs.datacat.model.RecordType;

import org.srs.datacat.shared.FlatDataset;
import org.srs.datacat.test.DbHarness;

import org.srs.datacat.model.dataset.DatasetOption;
import org.srs.datacat.model.security.DcAclEntry;
import org.srs.datacat.model.security.DcAclEntryScope;
import org.srs.datacat.model.security.DcGroup;
import org.srs.datacat.model.security.DcPermissions;
import org.srs.datacat.shared.Provider;

import org.srs.vfs.PathUtils;

/**
 *
 * @author bvan
 */
public class DcFileSystemProviderTest {
    
    static DbHarness harness;
    public DcFileSystemProvider provider;
    
    public DcFileSystemProviderTest() throws IOException{ 
        DAOFactory factory = new DAOFactoryMySQL(harness.getDataSource());
        ModelProvider modelProvider = new Provider();
        this.provider  = new DcFileSystemProvider(factory, modelProvider);
    }
    
    @BeforeClass
    public static void setUpDb() throws SQLException, IOException{
        harness = DbHarness.getDbHarness();
        DAOFactory factory = new DAOFactoryMySQL(harness.getDataSource());
        ModelProvider modelProvider = new Provider();
        addRecords(factory, modelProvider);
    }
    
    @AfterClass
    public static void tearDownDb() throws Exception{
        harness = DbHarness.getDbHarness();
        DAOFactory factory = new DAOFactoryMySQL(harness.getDataSource());
        removeRecords(factory.newContainerDAO());
    }
    
    public static void addRecords(DAOFactory factory, ModelProvider provider) throws SQLException, IOException{
        try (BaseDAO dao = factory.newBaseDAO()){
            getDatacatObject(dao, DbHarness.TEST_BASE_PATH);
            return;
        } catch (NoSuchFileException x){ }
        
        try(ContainerDAO dao = factory.newContainerDAO()) {
            DatacatNode container = (DatacatNode) provider.getContainerBuilder()
                    .name(DbHarness.TEST_BASE_NAME).type(RecordType.FOLDER)
                    .build();
            DatasetContainer rootRecord = (DatasetContainer) provider
                    .getContainerBuilder().pk(0L).path("/").build();
            dao.createNode(rootRecord, DbHarness.TEST_BASE_NAME, container);
            dao.commit();            
        }
    }
    
    public static void removeRecords(ContainerDAO dao) throws Exception {
        /*DatacatNode folder = getDatacatObject(dao, DbHarness.TEST_BASE_PATH);
        dao.deleteFolder(folder.getPk());
        dao.commit();
        dao.close();*/
    }
    
    public static DatacatNode getDatacatObject(BaseDAO dao, String path) throws IOException, NoSuchFileException {
        if(!PathUtils.isAbsolute( path )){
            path = "/" + path;
        }
        path = PathUtils.normalize( path );
        DatacatNode next = dao.getObjectInParent(null, "/");
        int offsets[] = PathUtils.offsets(path);
        for(int i = 1; i <= offsets.length; i++){
            next = dao.getObjectInParent(next, PathUtils.getFileName(PathUtils.absoluteSubpath(path, i, offsets)));
        }
        return next;
    }
    
    @Test
    public void testCacheStream() throws IOException{
        Path rootPath = provider.getPath("/");
        try(DirectoryStream<Path> s = provider.newOptimizedDirectoryStream(rootPath, TestUtils.DEFAULT_TEST_CONTEXT,
                DcFileSystemProvider.ACCEPT_ALL_FILTER, Integer.MAX_VALUE, Optional.of(DatasetView.EMPTY))){
            for(Path p: s){
                // Do nothing
            }
        }

        DatacatRecord o = provider.getFile(rootPath.resolve("testpath"), TestUtils.DEFAULT_TEST_CONTEXT).getObject();
        long t0 = System.currentTimeMillis();
        try(DirectoryStream<Path> cstream = provider.unCachedDirectoryStream(rootPath.resolve("testpath"), DcFileSystemProvider.ACCEPT_ALL_FILTER, Optional.of(DatasetView.EMPTY), true )){
            for(Iterator<Path> iter = cstream.iterator(); iter.hasNext();){
                iter.next();
            }
        }
        System.out.println("uncached directory stream took:" + (System.currentTimeMillis() - t0));
        
        t0 = System.currentTimeMillis();
        for(int i = 0; i <100; i++){
            try(DirectoryStream<Path> cstream = provider.newOptimizedDirectoryStream( rootPath.resolve("testpath"), TestUtils.DEFAULT_TEST_CONTEXT,
                    DcFileSystemProvider.ACCEPT_ALL_FILTER, Integer.MAX_VALUE, Optional.of(DatasetView.EMPTY))){
                for(Iterator<Path> iter = cstream.iterator(); iter.hasNext();){
                    iter.next();
                }
            }   
        }
        System.out.println("100 cached directory streams took:" + (System.currentTimeMillis() - t0));
        
        /*Files.walkFileTree( rootPath.resolve("testpath"), new SimpleFileVisitor<Path>() {
            int filesVisited = 0;
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException{
                if(filesVisited > 50){
                    return FileVisitResult.TERMINATE;
                }
                filesVisited++;
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e)
                    throws IOException{
                if(e == null){
                    return FileVisitResult.CONTINUE;
                } else {
                    // directory iteration failed
                    throw e;
                }
            }
        } );*/
    }

    @Test
    public void testCreateDataset() throws IOException{        
        DatasetModel.Builder builder = provider.getModelProvider().getDatasetBuilder();
        builder.name("testCaseDataset001");
        builder.dataType(DbHarness.TEST_DATATYPE_01);
        builder.fileFormat(DbHarness.TEST_FILEFORMAT_01);
        builder.datasetSource( DbHarness.TEST_DATASET_SOURCE);
        
        DatasetModel request = builder.build();
        Path parentPath = provider.getPath(DbHarness.TEST_BASE_PATH);
        Path filePath = parentPath.resolve(request.getName());
        HashSet<DatasetOption> options = new HashSet<>(Arrays.asList( DatasetOption.CREATE_NODE));
        provider.createDataset( filePath, TestUtils.DEFAULT_TEST_CONTEXT, request, options);
        provider.delete(filePath, TestUtils.DEFAULT_TEST_CONTEXT);
    }
    
    @Test
    public void testCreateDeleteDirectory() throws IOException {
        
        String folderName = "createFolderTest";
        DatasetContainer request = (DatasetContainer) provider.getModelProvider().getContainerBuilder()
                .name(folderName)
                .parentPk(0L)
                .type(RecordType.FOLDER)
                .build();

        Path path =  provider.getPath(DbHarness.TEST_BASE_PATH);
        provider.createDirectory(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT, request);
        provider.createDirectory(path.resolve(folderName).resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT, request);
        
        // directory not empty
        try {
            provider.delete(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
            TestCase.fail( "Should have failed deleting directory");
        } catch (DirectoryNotEmptyException ex){}
        
        provider.delete(path.resolve(folderName).resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
        provider.delete(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
        
        try {
            // File should already be deleted
            provider.delete(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
        } catch (NoSuchFileException ex){}
    }
    
    
    @Test
    public void testDirectoryAcl() throws IOException {
        
        String folderName = "directoryAclTest";
        DatasetContainer request = (DatasetContainer) provider.getModelProvider().getContainerBuilder()
                .name(folderName)
                .parentPk(0L)
                .type(RecordType.FOLDER)
                .build();

        Path path =  provider.getPath(DbHarness.TEST_BASE_PATH);
        provider.createDirectory(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT, request);
        provider.createDirectory(path.resolve(folderName).resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT, request);
        
        try {
            
            Path target = path.resolve(folderName);
            List<DcAclEntry> newAcl = new ArrayList<>();
            DcAclEntry entry = DcAclEntry.newBuilder()
                .subject(DcGroup.PUBLIC_GROUP)
                .permissions("rw")
                .scope(DcAclEntryScope.DEFAULT)
                .build();
            newAcl.add(entry);
            provider.mergeContainerAclEntries(target, TestUtils.DEFAULT_TEST_CONTEXT, newAcl, false);
            
            boolean okay = false;
            for(DcAclEntry e: provider.getAcl(target, TestUtils.DEFAULT_TEST_CONTEXT)){
                if(e.getSubject().equals(DcGroup.PUBLIC_GROUP)){
                    if(e.getPermissions().contains(DcPermissions.WRITE)){
                        okay = true;
                        break;
                    }
                }
            }
            
            newAcl = new ArrayList<>();
            entry = DcAclEntry.newBuilder()
                .subject(DcGroup.PUBLIC_GROUP)
                .permissions(Collections.EMPTY_SET)
                .scope(DcAclEntryScope.DEFAULT)
                .build();

            newAcl.add(entry);
            provider.mergeContainerAclEntries(target, TestUtils.DEFAULT_TEST_CONTEXT, newAcl, false);
            
            okay = true;
            for(DcAclEntry e: provider.getAcl(target, TestUtils.DEFAULT_TEST_CONTEXT)){
                if(e.getSubject().equals(DcGroup.PUBLIC_GROUP)){
                    if(e.getPermissions().contains(DcPermissions.WRITE)){
                        okay = false;
                        break;
                    }
                }
            }
            
            TestCase.assertTrue("Should not have passed check", okay);
            
            newAcl = new ArrayList<>();
            entry = DcAclEntry.newBuilder()
                .subject(DcGroup.PUBLIC_GROUP)
                .permissions("rwd")
                .scope(DcAclEntryScope.DEFAULT)
                .build();

            newAcl.add(entry);
            provider.mergeContainerAclEntries(target, TestUtils.DEFAULT_TEST_CONTEXT, newAcl, true);
            TestCase.assertEquals("Only one entry should exist", 1, provider.getAcl(target, TestUtils.DEFAULT_TEST_CONTEXT).size());
            
            provider.getAcl(path, TestUtils.DEFAULT_TEST_CONTEXT);
            TestCase.assertEquals("ridwa", provider.getPermissions(path, TestUtils.DEFAULT_TEST_CONTEXT, null));
            TestCase.assertEquals("ridwa", provider.getPermissions(path, TestUtils.DEFAULT_TEST_CONTEXT, new DcGroup("test_group@SRS")));
            TestCase.assertEquals("r", provider.getPermissions(path, TestUtils.DEFAULT_TEST_CONTEXT, DcGroup.PUBLIC_GROUP));
            TestCase.assertEquals("r", provider.getPermissions(path, TestUtils.DEFAULT_TEST_CONTEXT, new DcGroup("bogus@")));
            
            
            /* TODO: Define this behavior - clear = true, permissions = empty
            newAcl = new ArrayList<>();
            entry = DcAclEntry.newBuilder()
                .subject(DcGroup.PUBLIC_GROUP)
                .permissions(Collections.EMPTY_SET)
                .scope(DcAclEntryScope.DEFAULT)
                .build();

            newAcl.add(entry);
            provider.mergeContainerAclEntries(target, newAcl, true);
            System.out.println(provider.getFile(target).getAcl());
            TestCase.assertEquals("Only one entry should exist", 1, provider.getFile(target).getAcl().size());
            */
            
        } catch (IOException ex){
            ex.printStackTrace();
        }

        provider.delete(path.resolve(folderName).resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
        provider.delete(path.resolve(folderName), TestUtils.DEFAULT_TEST_CONTEXT);
    }


    @Test
    public void testCreateDeleteDependency() throws IOException {

        //build dataset1

        DatasetModel.Builder builder1 = provider.getModelProvider().getDatasetBuilder();
        builder1.name("testCaseDataset001");
        builder1.dataType(DbHarness.TEST_DATATYPE_01);
        builder1.fileFormat(DbHarness.TEST_FILEFORMAT_01);
        builder1.datasetSource( DbHarness.TEST_DATASET_SOURCE);
        builder1.versionId(DatasetView.NEW_VER);

        //adding versionMetadata to dataset1
        HashMap<String,Object> metadata1 = new HashMap<>();
        metadata1.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata1.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);

        builder1.versionMetadata(metadata1);

        DatasetModel request1 = builder1.build();
        Path parentPath1 = provider.getPath(DbHarness.TEST_BASE_PATH);
        Path filePath1 = parentPath1.resolve(request1.getName());
        HashSet<DatasetOption> options1 = new HashSet<>(Arrays.asList( DatasetOption.CREATE_NODE,DatasetOption.CREATE_VERSION));
        DatasetModel m = provider.createDataset( filePath1, TestUtils.DEFAULT_TEST_CONTEXT, request1, options1);
        //getting dataset1 versionPk
        String ds1VersionPk = String.valueOf(((FlatDataset) m).getVersionPk());
        System.out.println(ds1VersionPk);

        //construct dependency metadata for dataset2
        HashMap<String,Object> metadata2 = new HashMap<>();
        metadata2.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata2.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        metadata2.put("dependencyName","test");
        metadata2.put("dependency","");
        metadata2.put("dependents", ds1VersionPk);
        metadata2.put("dependentType","predecessor");


        //build dataset2
        DatasetModel.Builder builder2 = provider.getModelProvider().getDatasetBuilder();
        builder2.name("testCaseDataset002");
        builder2.dataType(DbHarness.TEST_DATATYPE_01);
        builder2.fileFormat(DbHarness.TEST_FILEFORMAT_01);
        builder2.datasetSource( DbHarness.TEST_DATASET_SOURCE);
        builder2.versionId(DatasetView.NEW_VER);
        builder2.versionMetadata(metadata2);

        DatasetModel request2 = builder2.build();
        Path parentPath2 = provider.getPath(DbHarness.TEST_BASE_PATH);
        Path filePath2 = parentPath2.resolve(request2.getName());
        HashSet<DatasetOption> options2 = new HashSet<>(Arrays.asList( DatasetOption.CREATE_NODE,DatasetOption.CREATE_VERSION));
        DatasetModel m2 = provider.createDataset( filePath2, TestUtils.DEFAULT_TEST_CONTEXT, request2, options2);




        //delete dataset1
        provider.delete(filePath1, TestUtils.DEFAULT_TEST_CONTEXT);
        //delete dataset2
        provider.delete(filePath2, TestUtils.DEFAULT_TEST_CONTEXT);


        //confirmation of dataset1 deletion
        try{
            Path rootPath = provider.getPath(DbHarness.TEST_BASE_PATH);
            DatacatRecord o = provider.getFile(rootPath.resolve(request1.getName()), TestUtils.DEFAULT_TEST_CONTEXT).getObject();
            System.out.println("Dataset1 is not deleted yet");
        } catch(NoSuchFileException ex){
            System.out.println("Dataset1 is deleted");
        }

        //confirmation of dataset2 deletion (Therefore deleting dependency)
        try{
            Path rootPath = provider.getPath(DbHarness.TEST_BASE_PATH);
            DatacatRecord o = provider.getFile(rootPath.resolve(request2.getName()), TestUtils.DEFAULT_TEST_CONTEXT).getObject();
            System.out.println("Dataset2 is not deleted yet");
        } catch(NoSuchFileException ex){
            System.out.println("Dataset2 is deleted");
        }



        /**
         * @author Chufan Wu
         * If you want to confirm that dependency is created in dataset2, use this script in Evaluate window
         *
         *         ResultSet rs = getConnection().createStatement().executeQuery("select * from DatasetDependency");
         *         System.out.println("\ndataset_dependency_table:");
         *
         *         while (rs.next()) {
         *             String dependencyOut = rs.getString("Dependency");
         *             System.out.println("Dependency:");
         *             System.out.println(dependencyOut);
         *
         *             String nameOut = rs.getString("Name");
         *             System.out.println("Name:");
         *             System.out.println(nameOut);
         *
         *             String dependentOut = rs.getString("Dependent");
         *             System.out.println("Dependent:");
         *             System.out.println(dependentOut);
         *
         *             String dependentTypeOut = rs.getString("DependentType");
         *             System.out.println("DependentType:");
         *             System.out.println(dependentTypeOut);
         *         }
         */

    }


}
