
package org.srs.datacat.dao.sql.search.tables;

import org.zerorm.core.Column;
import org.zerorm.core.Table;
import org.zerorm.core.interfaces.Schema;

/**
 *
 * @author kennylo
 */
@Schema(name = "DatasetGroup")
public class DatasetGroup extends Table {
    @Schema public Column<Long> datasetGroup;
    @Schema public Column<String> name;
    @Schema public Column<Long> datasetLogicalFolder;
    @Schema public Column<String> description;

    public DatasetGroup(){
        super();
    }
}

