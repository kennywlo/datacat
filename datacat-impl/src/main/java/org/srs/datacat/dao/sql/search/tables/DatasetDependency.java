package org.srs.datacat.dao.sql.search.tables;

import org.zerorm.core.Column;
import org.zerorm.core.Table;
import org.zerorm.core.interfaces.Schema;

/**
 *
 * @author klo
 */
@Schema(name = "DatasetDependency")
public class DatasetDependency extends Table {
    @Schema public Column<Long> dependency;
    @Schema public Column<String> pathName;
    @Schema public Column<Long> dependent;
    @Schema public Column<String> dependentType;

    public DatasetDependency(){
        super();
    }
}

