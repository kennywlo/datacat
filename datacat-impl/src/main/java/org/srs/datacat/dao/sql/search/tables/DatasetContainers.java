
package org.srs.datacat.dao.sql.search.tables;

import java.sql.Timestamp;
import java.util.Map;

import org.zerorm.core.Column;
import org.zerorm.core.Select;

/**
 *
 * @author bvan
 */
public class DatasetContainers extends MetajoinedStatement {
    public LogicalFolder lf = new LogicalFolder().as("lf", LogicalFolder.class );
    public DatasetGroup dsg = new DatasetGroup().as("dsg", DatasetGroup.class );
    boolean isGroup;

    public DatasetContainers(boolean isGroup){
        this.isGroup = isGroup;
        if (!this.isGroup) {
            lf.datasetLogicalFolder.as("pk");
            from(lf).selection(lf.getColumns());
        } else {
            dsg.datasetGroup.as("pk");
            from(dsg).selection(dsg.getColumns());
        }
    }

    @Override
    public Metatable getMetatableForType(String alias, Class type){
        Metatable ms = null;
        if(Number.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type)){
            ms = new LogicalFolderMetanumber().as(alias, Metatable.class);
        } else if(String.class.isAssignableFrom(type)){
            ms = new LogicalFolderMetastring().as(alias, Metatable.class);
        } else if(Timestamp.class.isAssignableFrom(type)){
            ms = new LogicalFolderMetatimestamp().as(alias, Metatable.class);
        }
        return ms;
    }
    
    @Override
    public Map<String, Select> getMetajoins(){
        return metaJoins;
    }

    @Override
    public Column getMetajoinColumn(){
        if (this.isGroup){
            return dsg.datasetGroup;
        } else {
            return lf.datasetLogicalFolder;
        }
    }
    
    @Override
    public String getMetanamePrefix(){
        return "dsc";
    }
}
