
package org.srs.datacat.sql;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.shared.DatacatObject;
import org.srs.datacat.shared.Dataset;
import org.srs.datacat.shared.DatasetGroup;
import org.srs.datacat.shared.DatasetLocation;
import org.srs.datacat.shared.DatasetVersion;
import org.srs.datacat.shared.LogicalFolder;
import org.srs.datacat.shared.container.BasicStat;
import org.srs.datacat.shared.container.DatasetStat;
import org.srs.datacat.shared.dataset.VersionWithLocations;
import org.srs.datacat.vfs.DcPath;
import org.srs.vfs.PathUtils;


/**
 *
 * @author bvan
 */
public class ContainerDAO extends BaseDAO {
    
    public static final int FETCH_SIZE_CHILDREN = 5000;
    public static final int FETCH_SIZE_METADATA = 100000;

    public ContainerDAO(Connection conn){
        super( conn );
    }
    
    public DatacatObject createContainer(Long parentPk, DcPath targetPath, DatacatObject request) throws SQLException{
        return insertContainer( parentPk, targetPath.toString(), request );
    }

    protected DatacatObject insertContainer(Long parentPk, String targetPath, DatacatObject request) throws SQLException{
        String name = PathUtils.getFileName(targetPath);
        String tableName;
        String parentColumn;
        DatacatObject.Type newType = request.getType();
        DatacatObject retObject;
        DatasetContainer.Builder builder;
        switch(newType){
            case FOLDER:
                builder = new LogicalFolder.Builder(request);
                tableName = "DATASETLOGICALFOLDER";
                parentColumn = "PARENT";
                break;
            case GROUP:
                builder = new DatasetGroup.Builder(request);
                tableName = "DATASETGROUP";
                parentColumn = "DATASETLOGICALFOLDER";
                break;
            default:
                throw new SQLException("Unknown parent table: " + newType.toString());
        }
        String insertSqlTemplate = "INSERT INTO %s (NAME, %s, DESCRIPTION) VALUES (?,?,?)";
        String sql = String.format( insertSqlTemplate, tableName, parentColumn );
        
        String description = request instanceof DatasetContainer ? ((DatasetContainer) request).getDescription() : null;
        
        try (PreparedStatement stmt = getConnection().prepareStatement( sql, new String[]{tableName} )) {
            stmt.setString( 1, name);
            stmt.setLong(2, parentPk);
            stmt.setString(3, description);
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()){
                rs.next();
                builder.pk(rs.getLong(1));
            }
            builder.parentPk(parentPk);
            builder.path(targetPath);
            retObject = builder.build();
        }
        
        if(request.getMetadata() != null && !request.getMetadata().isEmpty()){
            if(newType == DatacatObject.Type.FOLDER){
                addFolderMetadata(retObject.getPk(), retObject.getNumberMetadata());
                addFolderMetadata(retObject.getPk(), retObject.getStringMetadata());
            } else {
                addGroupMetadata(retObject.getPk(), retObject.getNumberMetadata());
                addGroupMetadata(retObject.getPk(), retObject.getStringMetadata());
            }
        }
        return retObject;
    }
    
    public void deleteContainer(DatacatObject container) throws SQLException, IOException{
        switch(container.getType()){
            case GROUP:
                deleteGroup(container.getPk());
                return;
            case FOLDER:
                deleteFolder(container.getPk());
                return;
        }
        throw new IOException("Unable to delete object: Not a Group or Folder" + container.getType());
    }
    
    protected void deleteFolder(long folderPk) throws SQLException {
        String deleteSql = "delete from DatasetLogicalFolder where DatasetLogicalFolder=?";
        delete1( deleteSql, folderPk);
    }
    
    protected void deleteGroup(long groupPk) throws SQLException {
        String deleteSql = "delete from DatasetGroup where DatasetGroup=?";
        delete1( deleteSql, groupPk);
    }
    
    public BasicStat getBasicStat(DatacatObject container) throws SQLException {
        String parent = container instanceof LogicalFolder ? "datasetlogicalfolder" : "datasetgroup";

        String statSQL = "select 'D' type, count(1) count from verdataset where " + parent + " = ? ";
        if(container instanceof LogicalFolder){
            statSQL = statSQL + "UNION ALL select 'G' type, count(1) count from datasetgroup where datasetlogicalfolder = ? ";
            statSQL = statSQL + "UNION ALL select 'F' type, count(1) count from datasetlogicalfolder where parent = ? ";
        }
        try(PreparedStatement stmt = getConnection().prepareStatement( statSQL )) {
            stmt.setLong( 1, ((DatacatObject) container).getPk() );
            if(container instanceof LogicalFolder){
                stmt.setLong( 2, ((DatacatObject) container).getPk() );
                stmt.setLong( 3, ((DatacatObject) container).getPk() );
            }
            ResultSet rs = stmt.executeQuery();
            BasicStat cs = new BasicStat();
            while(rs.next()){
                Integer count = rs.getInt( "count");
                switch(getType(rs.getString( "type" ))){
                    case DATASET:
                        cs.setDatasetCount( count );
                        break;
                    case GROUP:
                        cs.setGroupCount( count );
                        break;
                    case FOLDER:
                        cs.setFolderCount( count );
                        break;
                }
            }
            return cs;
        }
    }
    
    
    public DatasetStat getDatasetStat(DatacatObject container, BasicStat stat) throws SQLException {
        String primaryTable;
        if(container instanceof LogicalFolder){
            primaryTable = "datasetlogicalfolder";
        } else {
            primaryTable = "datasetgroup";
        }
        
        String statSQL = 
                "select count(*) files, Sum(l.NumberEvents) events, "
                + "Sum(l.filesizebytes) totalsize, min(l.runMin) minrun, max(l.runmax) maxrun "
                + "from " + primaryTable + " g "
                + "join verdataset d on (g." + primaryTable + " =d." + primaryTable + ") "
                + "join datasetversion dv on (d.latestversion=dv.datasetversion) "
                + "join verdatasetlocation l on (dv.masterLocation=l.datasetlocation) "
                + "where g." + primaryTable + " = ? ";
        
        try(PreparedStatement stmt = getConnection().prepareStatement( statSQL )) {
            stmt.setLong( 1, container.getPk() );
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()){
                throw new SQLException("Unable to determine dataset stat");
            }
            DatasetStat ds = new DatasetStat(stat);
            ds.setDatasetCount( rs.getInt("files") );
            ds.setEventCount( rs.getLong( "events") );
            ds.setDiskUsageBytes( rs.getLong( "totalsize") );
            ds.setRunMin( rs.getLong( "minrun") );
            ds.setRunMax( rs.getLong( "maxrun") );
            return ds;
        }
    }
    
    public DirectoryStream<DatacatObject> getSubdirectoryStream(Long parentPk, final String parentPath) throws SQLException, IOException{
        return getChildrenStream( parentPk, parentPath, null);
    }
       
    public DirectoryStream<DatacatObject> getChildrenStream(Long parentPk, final String parentPath, 
            DatasetView viewPrefetch) throws SQLException, IOException{
        String sql = "WITH OBJECTS (type, pk, name, parent) AS ( "
                + "    SELECT 'F', datasetlogicalfolder, name, parent "
                + "      FROM datasetlogicalfolder "
                + "  UNION ALL "
                + "    SELECT 'G', datasetGroup, name, datasetLogicalFolder "
                + "      FROM DatasetGroup "
                + ( viewPrefetch != null ? "  UNION ALL "
                + "    SELECT   'D', dataset, datasetName, "
                + "      CASE WHEN datasetlogicalfolder is not null THEN datasetlogicalfolder else datasetgroup END "
                + "      FROM VerDataset " : " " )
                + ") "
                + "SELECT type, pk, name, parent FROM OBJECTS "
                + "  WHERE parent = ? "
                + "  ORDER BY name";
        
        final PreparedStatement stmt = getConnection().prepareStatement( sql );
        final PreparedStatement prefetchVer;
        final PreparedStatement prefetchLoc;
        stmt.setLong( 1, parentPk);
        
        if(viewPrefetch != null){
            prefetchVer = getConnection()
                    .prepareStatement(getVersionsSql(VersionParent.CONTAINER, viewPrefetch));
            prefetchVer.setLong( 1,parentPk);
            if(!viewPrefetch.isCurrent()){
                prefetchVer.setInt( 2, viewPrefetch.getVersionId());
            }
            if(!DatasetView.EMPTY_SITES.equals(viewPrefetch.getSite())){
                prefetchLoc = getConnection()
                        .prepareStatement(getLocationsSql(VersionParent.CONTAINER, viewPrefetch));
                prefetchLoc.setLong( 1,parentPk);
                if(!viewPrefetch.isCurrent()){
                    prefetchLoc.setInt( 2, viewPrefetch.getVersionId());
                }
            } else {
                prefetchLoc = null;
            }
        } else {
            prefetchVer = null;
            prefetchLoc = null;
        }

        final ResultSet rs = stmt.executeQuery();
        rs.setFetchSize(FETCH_SIZE_CHILDREN);
        final ResultSet rsVer = prefetchVer != null ? prefetchVer.executeQuery() : null;
        final ResultSet rsLoc = prefetchLoc != null ? prefetchLoc.executeQuery() : null;
        DirectoryStream<DatacatObject> stream = new DirectoryStream<DatacatObject>() {
            Iterator<DatacatObject> iter = null;

            @Override
            public Iterator<DatacatObject> iterator(){
                if(iter == null){
                    iter = new Iterator<DatacatObject>() {

                        boolean beforeStart = true;
                        boolean wasOkay = false;
                        boolean consumed = false;
                        
                        @Override
                        public boolean hasNext(){
                            try{
                                return checkNext();
                            } catch (SQLException | NoSuchElementException ex){
                                return false;
                            }
                        }

                        private boolean checkNext() throws SQLException {
                            if(beforeStart || (wasOkay && consumed)){
                                consumed = false;
                                beforeStart = false;
                                wasOkay = rs.next();
                            }
                            return wasOkay;
                        }

                        @Override
                        public DatacatObject next(){
                            if(!hasNext()){
                                throw new NoSuchElementException();
                            }
                            try {
                                DatacatObject.Builder builder = getBuilder(rs, parentPath);
                                if(builder instanceof Dataset.Builder){
                                    checkResultSet((Dataset.Builder) builder, rsVer, rsLoc);
                                }
                                consumed = true;
                                return builder.build();
                            } catch(SQLException ex) {
                                throw new RuntimeException(ex);
                            }
                        }

                        @Override
                        public void remove(){throw new UnsupportedOperationException();}

                    };
                }
                return iter;
            }

            @Override
            public void close() throws IOException{
                try {
                    stmt.close();
                    if(prefetchVer!=null){
                        prefetchVer.close();
                    }
                    if(prefetchLoc != null){
                        prefetchLoc.close();
                    }
                } catch(SQLException ex) {
                    throw new IOException("Error closing statement", ex);
                }
            }
        };
        
        return stream;
    }
    
    private static void checkResultSet(Dataset.Builder dsBuilder, ResultSet dsVer, ResultSet dsLoc) throws SQLException {
        long dsPk = dsBuilder.pk;
        if(dsVer== null || dsVer.isClosed()){
            return;
        }
        if(dsVer.getRow() == 0){ 
            dsVer.setFetchSize(FETCH_SIZE_METADATA);
            if(!dsVer.next()){
                dsVer.close();
                return; 
            }
        }
        if(dsLoc != null && dsLoc.getRow() == 0){
            dsLoc.setFetchSize(FETCH_SIZE_CHILDREN);
            if(!dsLoc.next()){
                dsLoc.close();
            }
        }
        List<DatasetVersion> versions = new ArrayList<>();
        VersionWithLocations.Builder builder = new VersionWithLocations.Builder();
        long verPk = dsVer.getLong( "datasetversion");
        
        while(!dsVer.isClosed() && dsVer.getLong("dataset") == dsPk && dsVer.getLong( "datasetversion") == verPk){
            HashMap<String, Number> nmap = new HashMap<>();
            HashMap<String, String> smap = new HashMap<>();
            List<DatasetLocation> locations = new ArrayList<>();
            builder.pk(verPk);
            builder.parentPk(dsPk);
            builder.versionId(dsVer.getInt("versionid"));
            builder.datasetSource(dsVer.getString( "datasetSource"));
            builder.latest(dsVer.getBoolean( "isLatest"));                
            while(!dsVer.isClosed() && dsVer.getLong("dataset") == dsPk && dsVer.getLong( "datasetversion" ) == verPk){
                // Process all metadata entries first, 1 or more rows per version
                BaseDAO.processMetadata( dsVer, nmap, smap ); 
                if(!dsVer.next()){
                    dsVer.close();
                }
            }
            /* After we've processed all result rows for each version, (mostly metadata)
               process all locations for this version */
            while(dsLoc != null && !dsLoc.isClosed() && dsLoc.getLong("datasetversion") == verPk){
                // Assume one location per row. Row could be null (LEFT OUTER JOIN)
                if(dsLoc.getString( "datasetsite") != null){ 
                    BaseDAO.processLocation( dsLoc, builder.pk, locations); 
                }
                if(!dsLoc.next()){
                    dsLoc.close();
                }
            }
            builder.numberMetadata( nmap );
            builder.stringMetadata( smap );
            builder.locations(locations);
            versions.add(builder.build());
        }
        if(versions.size() == 1){
            dsBuilder.version(versions.get(0));
        } else {
            dsBuilder.versions(versions);
        }
    }

}
