package org.srs.datacat.dao;

import com.google.common.base.Optional;
import org.srs.datacat.model.*;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.model.dataset.DatasetOption;
import org.srs.datacat.model.dataset.DatasetVersionModel;
import org.srs.datacat.model.dataset.DatasetViewInfoModel;
import org.srs.datacat.test.DbHarness;
import org.srs.datacat.test.HSqlDbHarness;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author bvan
 */
public class DAOTestUtils {

    public static void generateDatasets(DAOFactory factory, int inFolders, int datasets) throws IOException{
        DatacatRecord tpObject = null;
        try(final BaseDAO dao = factory.newBaseDAO()) {
            tpObject = dao.getObjectInParent(dao.getObjectInParent(null, "/"), "testpath");
        }

        generateFolders(factory, inFolders);

        List opts = Arrays.asList(DatasetOption.CREATE_NODE, DatasetOption.CREATE_VERSION, DatasetOption.SKIP_NODE_CHECK);
        HashSet<DatasetOption> options = new HashSet<>(opts);
        for(int i = 0; i < inFolders; i++){
            String parentName = String.format("folder%05d", i);
            DatacatRecord parentObject = null;
            try(DatasetDAO dao = factory.newDatasetDAO()) {
                parentObject = dao.getObjectInParent(tpObject, parentName);
            
                for(int j = 0; j < datasets; j++){
                    String name = String.format("dataset%05d", j);
                    DatasetModel ds = mock(DatasetModel.class);
                    DatasetViewInfoModel view = mock(DatasetViewInfoModel.class);
                    DatasetVersionModel version = mock(DatasetVersionModel.class);
                    HashMap<String, Object> metadata = new HashMap<>();
                    metadata.put(DbHarness.numberName, DbHarness.numberMdValues[i % 4]);
                    metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[j % 4]);
                    
                    when(ds.getName()).thenReturn(name);
                    when(ds.getDataType()).thenReturn(HSqlDbHarness.JUNIT_DATASET_DATATYPE);
                    when(ds.getFileFormat()).thenReturn(HSqlDbHarness.JUNIT_DATASET_FILEFORMAT);
                    
                    when(version.getVersionId()).thenReturn(DatasetView.NEW_VER);
                    when(version.getMetadataMap()).thenReturn(metadata);
                    when(version.getDatasetSource()).thenReturn(HSqlDbHarness.JUNIT_DATASET_DATASOURCE);
                    
                    when(view.versionOpt()).thenReturn(Optional.of(version));
                    when(view.getVersion()).thenReturn(version);
                    when(view.locationsOpt()).thenReturn(Optional.<Set<DatasetLocationModel>>absent());

                    dao.createDataset(parentObject, name, Optional.of(ds), Optional.of(view), options);
                }
                dao.commit();
            }
        }
    }

    public static void generateFolders(DAOFactory factory, int folders) throws IOException{
        DatacatRecord tpObject = null;
        try(ContainerDAO dao = factory.newContainerDAO()) {
            tpObject = dao.getObjectInParent(dao.getObjectInParent(null, "/"), "testpath");
            for(int i = 0; i < folders; i++){
                String name = String.format("folder%05d", i);
                DatasetContainer cont = mock(DatasetContainer.class);
                when(cont.getType()).thenReturn(RecordType.FOLDER);
                dao.createNode(tpObject, name, cont);
            }
            dao.commit();
        }
    }

    public static List<DatacatNode> getFolders(DAOFactory factory, int folders) throws IOException{
        List<DatacatNode> ret = new ArrayList<>();
        try(ContainerDAO dao = factory.newContainerDAO()) {
            DatacatRecord tpObject = dao.getObjectInParent(dao.getObjectInParent(null, "/"), "testpath");

            for(int i = 0; i < folders; i++){
                String name = String.format("folder%05d", i);
                ret.add(dao.getObjectInParent(tpObject, name));
            }
            dao.commit();
        }
        return ret;
    }
    
    public static List<DatacatNode> getContainers(DAOFactory factory) throws IOException{
        List<DatacatNode> ret = new ArrayList<>();
        try(ContainerDAO dao = factory.newContainerDAO()) {
            DatacatRecord tpObject = dao.getObjectInParent(dao.getObjectInParent(null, "/"), "testpath");
                
            try (DirectoryStream<DatacatNode> stream = dao.getChildrenStream(tpObject, Optional.<DatasetView>absent())){
                for(DatacatNode node: stream){
                    if(node.getType().isContainer()){
                        ret.add(node);
                    }
                }
            }
        }
        return ret;
    }


    public static HashMap<String, Object> generateDependencies(String dependencyNamePT, String dependentsPT, String dependentTypePT, HashMap<String, Object> metadataPT) throws IOException{
        /**
         * @author CesarAxel
         */
        /**
         * Generates a dependency hashmap using provided dependency fields.
         * @param dependencyNamePT      The name of the dependency
         * @param dependentsPT      A comma delimited string containing the version PKs of all Datasets the current DS is dependent of
         * @param dependentTypePT       The number of dataset you want to create
         * @param metadataPT    HashMap containing current metadata fields and values
         * @return          An updated version of metadataPT containing dependency MetaData
         * @author ChufanWu
         */

        metadataPT.put("dependencyName", dependencyNamePT);
        metadataPT.put("dependency","");
        metadataPT.put("dependents", dependentsPT);
        metadataPT.put("dependentType", dependentTypePT);

        return metadataPT;

    }

}
