package org.srs.datacatalog.search.plugins;

import java.util.HashMap;
import org.zerorm.core.Column;
import org.zerorm.core.Select;
import org.zerorm.core.Table;
import org.zerorm.core.interfaces.Schema;
import org.zerorm.core.interfaces.SimpleTable;
import org.srs.datacatalog.search.tables.DatasetVersions;

/**
 *
 * @author bvan
 */
public class LsstFilesSearchPlugin implements DatacatPlugin {
    
    @Schema(name="jgates_fitsTest.FitsFiles", alias="lsstff")
    class FitsFiles extends Table {            
        @Schema(name="fitsFileId") 
        public Column<Long> fileId;
        @Schema(name="fileName") 
        public Column<String> fileName;
        @Schema(name="hduCount")
        public Column<Long> hduCount;

        
        public FitsFiles(){ super(); }
        
    };
    
    final private String namespace = "files";
    FitsFiles lsstff = new FitsFiles();
    private boolean joined;
    
    private HashMap<String, Column> mappings = new HashMap<>();
    
    public LsstFilesSearchPlugin(){

        for(Column c: new FitsFiles().getColumns()){
            mappings.put( c.canonical(), c);
        }
    }

    public String getNamespace(){
        return this.namespace;
    }

    public boolean containsKey(String key){
        return mappings.containsKey( key );
    }

    public SimpleTable joinToStatement(String key, Select statement){
        if(joined){
            return lsstff;
        }
        String metadataPivot = "fileId";
        DatasetVersions dsv = (DatasetVersions) statement;
        Column vecColumn = dsv.setupMetadataOuterJoin( metadataPivot,  Number.class );

        dsv.selection( lsstff.getColumns() ).leftOuterJoin( lsstff, vecColumn.eq( lsstff.fileId )  );
        joined = true;
        return lsstff;
    }

}
