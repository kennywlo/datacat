
package org.srs.datacat.model;

import org.srs.datacat.model.container.ContainerStat;

/**
 * An interface denoting either a folder, a group, or a dependency.
 * 
 * @author bvan
 *
 */
public interface DatasetContainer extends DatacatNode, HasMetadata {
    
    ContainerStat getStat();
    String getDescription();

    /**
     * Dependency interface.
     */
    public interface Dependency extends DatasetContainer{}

    /**
     * Folder interface.
     */
    public interface Folder extends DatasetContainer{}
    
    /**
     * Group interface.
     */
    public interface Group extends DatasetContainer{}

}
