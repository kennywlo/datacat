
package org.srs.datacat.model;

import org.srs.datacat.model.container.ContainerStat;

/**
 * An interface denoting either a folder or a group.
 * 
 * @author bvan
 * @author klo
 */
public interface DatasetContainer extends DatacatNode, HasMetadata {
    
    ContainerStat getStat();
    String getDescription();
    
    /**
     * Folder interface.
     */
    public interface Folder extends DatasetContainer{}
    
    /**
     * Group interface.
     */
    public interface Group extends DatasetContainer{}

    /**
     * Dependency interface.
     */
    public interface Dependency extends DatasetContainer{}

}
