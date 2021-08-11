package org.srs.datacat.dao.sql.search.plugins;

import java.util.HashMap;

import org.srs.datacat.dao.sql.search.tables.DatasetVersions;
import org.zerorm.core.Column;
import org.zerorm.core.Table;
import org.zerorm.core.interfaces.Schema;
import org.zerorm.core.interfaces.SimpleTable;
import org.srs.datacat.dao.sql.search.tables.MetajoinedStatement;

/**
 *
 * @author klo
 */
public class DependencySearchPlugin implements DatacatPlugin {

    @Schema(name="DatasetDependency", alias="dependency")
    class Dependency extends Table {

        @Schema public Column<Long> dependency;
        @Schema public Column<String> dependencyName;
        @Schema public Column<Long> dependent;
        @Schema public Column<String> dependentType;

        public Dependency(){ super(); }

    };

    private static final String NAMESPACE = "deps";
    Dependency dependents = new Dependency();
    private boolean joined;

    private HashMap<String, Column> mappings = new HashMap<>();

    public DependencySearchPlugin(){

        for(Column c: new Dependency().getColumns()){
            mappings.put(c.canonical(), c);
        }
    }

    @Override
    public String getNamespace(){
        return NAMESPACE;
    }

    @Override
    public boolean containsKey(String key){
        return mappings.containsKey( key );
    }

    @Override
    public SimpleTable joinToStatement(String key, MetajoinedStatement statement){
        if(joined){
            return dependents;
        }
        String metadataPivot = "dependents";
        DatasetVersions dsv = (DatasetVersions) statement;
        // Column vecColumn = dsv.setupMetadataOuterJoin(metadataPivot, Number.class);
        Column vecColumn = new Column(metadataPivot, null);
        dsv.selection(dependents.getColumns()).leftOuterJoin(dependents, vecColumn.eq(dependents.dependency));
        joined = true;
        return dependents;
    }

}
