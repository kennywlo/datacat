
package org.srs.datacat.model;

import java.util.Map;
import org.srs.datacat.model.container.ContainerStat;
import org.srs.datacat.model.container.DatasetContainerBuilder;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.model.dataset.DatasetVersionModel;

/**
 * Interface to provide an application with a  model implementation.
 * @author bvan
 */
public interface ModelProvider {
    
    DatacatNode.DatacatNodeBuilder getNodeBuilder();
    DatacatRecord.DatacatRecordBuilder getRecordBuilder();
    DatasetModel.Builder getDatasetBuilder();
    DatasetContainerBuilder getContainerBuilder();
    Class<? extends ContainerStat> getStatByName(String name);
    DatasetVersionModel.Builder getVersionBuilder();
    DatasetLocationModel.Builder getLocationBuilder();
    DatasetResultSetModel.Builder getDatasetResultSetBuilder();
    ContainerResultSetModel.Builder getContainerResultSetBuilder();

    Map<Class, Class> modelProviders();
    
}
