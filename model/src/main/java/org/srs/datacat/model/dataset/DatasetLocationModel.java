
package org.srs.datacat.model.dataset;

import java.sql.Timestamp;
import org.srs.datacat.model.DatacatRecord;

/**
 * DatasetLocationModel is the model interface for a location.
 * @author bvan
 */
public interface DatasetLocationModel extends DatacatRecord {

    String getResource();
    Long getSize();
    String getChecksum();
    Timestamp getDateModified();
    Timestamp getDateCreated();
    Timestamp getDateScanned();
    String getSite();
    String getScanStatus();
    Boolean isMaster();
    
    /**
     * Version Builder interface.
     * @param <U> Implementation class.
     */
    public interface Builder<U extends Builder> extends DatacatRecordBuilder<U> {
        
        @Override
        DatasetLocationModel build();
        U create(DatasetLocationModel val);
        U site(String val);
        U resource(String val);
        U size(Long val);
        U checksum(String val);
        U created(Timestamp val);
        U modified(Timestamp val);
        U scanStatus(String val);
        U master(Boolean val);
    }
}
