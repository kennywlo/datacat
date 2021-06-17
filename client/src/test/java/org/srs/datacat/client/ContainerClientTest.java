
package org.srs.datacat.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.ModelProvider;
import org.srs.datacat.model.RecordType;
import org.srs.datacat.shared.Provider;
import org.srs.datacat.test.DbHarness;

/**
 *
 * @author bvan
 */
public class ContainerClientTest {

    public static void generateFolders(Client testClient, int folders) throws IOException{
        String parent = "/testpath";
        // Create n folders
        for(int i = 0; i < folders; i++){
            String name =String.format("folder%05d", i);
            ModelProvider provider = new Provider();
            DatasetContainer newContainer = (DatasetContainer) provider.getContainerBuilder()
                    .name(name)
                    .type(RecordType.FOLDER)
                    .build();

            testClient.createContainer(parent, newContainer);
        }
    }


    /**
     * Generate and return dependencies metadata
     * @author Chufan Wu
     */
    public static HashMap<String, Object> generateDependencies(String versionPk) throws IOException{

        HashMap<String, Object> metadata = new HashMap<>();
        metadata.put(DbHarness.numberName, DbHarness.numberMdValues[0]);
        metadata.put(DbHarness.alphaName, DbHarness.alphaMdValues[0]);
        metadata.put("dependencyName","test");
        metadata.put("dependency","");
        metadata.put("dependents",versionPk);
        metadata.put("dependentType","predecessor");


        return metadata;
    }

}
