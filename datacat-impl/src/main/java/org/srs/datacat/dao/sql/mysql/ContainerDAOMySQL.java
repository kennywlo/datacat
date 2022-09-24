package org.srs.datacat.dao.sql.mysql;

import com.google.common.base.Optional;
import org.srs.datacat.dao.sql.SqlBaseDAO;
import org.srs.datacat.dao.sql.SqlDAOFactory;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.*;
import org.srs.datacat.model.container.ContainerStat;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.shared.*;
import org.srs.vfs.PathUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author bvan
 */
public class ContainerDAOMySQL extends BaseDAOMySQL implements org.srs.datacat.dao.ContainerDAO {

    public static final int FETCH_SIZE_CHILDREN = 5000;
    public static final int FETCH_SIZE_METADATA = 100000;

    public ContainerDAOMySQL(Connection conn, SqlDAOFactory.Locker locker){
        super(conn, locker);
    }

    public DatasetContainer createContainer(DatacatRecord parent, String name, 
            DatasetContainer request) throws IOException{
        try {
            return insertContainer(parent, name, request);
        } catch(SQLException ex) {
            throw new IOException("Unable to create container: " + PathUtils.resolve(parent.
                    getPath(), name), ex);
        }
    }
    
    protected DatasetContainer insertContainer(DatacatRecord parent, String name,
            DatasetContainer request) throws SQLException{
        String tableName;
        String parentColumn;
        RecordType newType = request.getType();
        DatasetContainer retObject;
        DatasetContainerBuilder builder;
        switch(newType){
            case FOLDER:
                builder = new LogicalFolder.Builder(request);
                tableName = "DatasetLogicalFolder";
                parentColumn = "PARENT";
                break;
            case GROUP:
                builder = new DatasetGroup.Builder(request);
                tableName = "DatasetGroup";
                parentColumn = "DATASETLOGICALFOLDER";
                break;
            default:
                throw new SQLException("Unknown parent table: " + newType.toString());
        }

        String insertSqlTemplate = "INSERT INTO %s (NAME, %s, DESCRIPTION) VALUES (?,?,?)";
        String sql = String.format(insertSqlTemplate, tableName, parentColumn);

        String description = request.getDescription();

        try(PreparedStatement stmt = getConnection().prepareStatement(sql, new String[]{tableName.toUpperCase()})) {
            stmt.setString(1, name);
            stmt.setLong(2, parent.getPk());
            stmt.setString(3, description);
            stmt.executeUpdate();
            try(ResultSet rs = stmt.getGeneratedKeys()) {
                rs.next();
                builder.pk(rs.getLong(1));
            }
            builder.parentPk(parent.getPk());
            builder.path(PathUtils.resolve(parent.getPath(), name));
            retObject = builder.build();
        }
        if(request.getMetadataMap() != null && !request.getMetadataMap().isEmpty()){
            if(newType == RecordType.FOLDER){
                addFolderMetadata(retObject.getPk(), retObject.getMetadataMap());
            } else {
                addGroupMetadata(retObject, retObject.getMetadataMap());
            }
        }
        return retObject;
    }

    public void deleteContainer(DatacatRecord container) throws IOException{
        try {
            switch(container.getType()){
                case GROUP:
                    deleteGroup(container.getPk());
                    return;
                case FOLDER:
                    deleteFolder(container.getPk());
                    return;
                default:
                    break;
            }
        } catch(SQLException ex) {
            throw new IOException("Unable to delete object: " + container.getPath(), ex);
        }
    }

    protected void deleteFolder(long folderPk) throws SQLException{
        String deleteSql = "delete from DatasetLogicalFolder where DatasetLogicalFolder=?";
        delete1(deleteSql, folderPk);
    }

    protected void deleteGroup(long groupPk) throws SQLException{
        String deleteSql = "delete from DatasetGroup where DatasetGroup=?";
        delete1(deleteSql, groupPk);
    }
    protected void deleteDependency(String dependency) throws SQLException{
        String deleteSql = "delete from DatasetDependency where DependencyName=?";
        delete1(deleteSql, dependency);
    }

    @Override
    public <V extends ContainerStat> V getStat(DatacatRecord container, Class<V> statType) throws IOException{
        if(statType == BasicStat.class || statType == ContainerStat.class){
            return (V) getBasicStat(container);
        }
        if(statType == DatasetStat.class){
            return (V) getDatasetStat(container);
        }
        throw new UnsupportedOperationException("Unsupported stat type");
    }

    public BasicStat getBasicStat(DatacatRecord container) throws IOException{
        boolean isFolder = container.getType() == RecordType.FOLDER;
        String parent = isFolder ? "DatasetLogicalFolder" : "DatasetGroup";

        String statSQL = "select 'D' type, count(1) cnt from VerDataset where " + parent + " = ? ";
        if(isFolder){
            statSQL = statSQL
                    + "UNION ALL select 'G' type, count(1) cnt from DatasetGroup where datasetlogicalfolder = ? ";
            statSQL = statSQL
                    + "UNION ALL select 'F' type, count(1) cnt from DatasetLogicalFolder where parent = ? ";
        }
        try(PreparedStatement stmt = getConnection().prepareStatement(statSQL)) {
            stmt.setLong(1, container.getPk());
            if(isFolder){
                stmt.setLong(2, container.getPk());
                stmt.setLong(3, container.getPk());
            }
            ResultSet rs = stmt.executeQuery();
            BasicStat cs = new BasicStat();
            while(rs.next()){
                Integer count = rs.getInt("cnt");
                switch(getType(rs.getString("type"))){
                    case DATASET:
                        cs.setDatasetCount(count);
                        break;
                    case GROUP:
                        cs.setGroupCount(count);
                        break;
                    case FOLDER:
                        cs.setFolderCount(count);
                        break;
                    default:
                        break;
                }
            }
            return cs;
        } catch(SQLException ex) {
            throw new IOException("Unable to stat container: " + container.getPath(), ex);
        }
    }

    public DatasetStat getDatasetStat(DatacatRecord container) throws IOException{
        String primaryTable;
        boolean isFolder = container.getType() == RecordType.FOLDER;
        if(isFolder){
            primaryTable = "DatasetLogicalFolder";
        } else {
            primaryTable = "DatasetGroup";
        }

        String statSQL = 
            "select count(*) files, Sum(l.NumberEvents) events, "
            + "Sum(l.filesizebytes) totalsize, min(l.runMin) minrun, max(l.runmax) maxrun "
            + "from " + primaryTable + " g "
            + "join VerDataset d on (g." + primaryTable + " =d." + primaryTable + ") "
            + "join DatasetVersion dv on (d.latestversion=dv.datasetversion) "
            + "join VerDatasetLocation l on (dv.masterLocation=l.datasetlocation) "
            + "where g." + primaryTable + " = ? ";

        try(PreparedStatement stmt = getConnection().prepareStatement(statSQL)) {
            stmt.setLong(1, container.getPk());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()){
                throw new SQLException("Unable to determine dataset stat");
            }
            DatasetStat ds = new DatasetStat(getBasicStat(container));
            ds.setDatasetCount(rs.getInt("files"));
            ds.setEventCount(rs.getLong("events"));
            ds.setDiskUsageBytes(rs.getLong("totalsize"));
            ds.setRunMin(rs.getLong("minrun"));
            ds.setRunMax(rs.getLong("maxrun"));
            return ds;
        } catch(SQLException ex) {
            throw new IOException("Unable to stat container: " + container.getPath(), ex);
        }
    }

    @Override
    public DirectoryStream<DatacatNode> getChildrenStream(DatacatRecord parent,
            Optional<DatasetView> viewPrefetch) throws IOException{
        try {
            return getChildrenStreamInternal(parent, viewPrefetch.orNull());
        } catch(SQLException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public void patchContainer(DatacatNode container, DatasetContainer request) throws IOException{
        try {
            patchContainerInternal(container, request);
        } catch (SQLException ex){
            throw new IOException("Unable to perform patch", ex);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex){
            throw new IOException("FATAL error in defined model", ex);
        }
    }
    
    protected void patchContainerInternal(DatacatNode container, DatasetContainer request) throws SQLException, 
            IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException{

        String table = container.getType()== RecordType.FOLDER ? "DatasetLogicalFolder" : "DatasetGroup";
        
        for(Method method: request.getClass().getMethods()){
            if(method.isAnnotationPresent(Patchable.class)){
                Object patchedValue = method.invoke(request);
                if(patchedValue == null){
                    continue;
                }
                if(patchedValue instanceof Map && ((Map) patchedValue).isEmpty()){
                    continue;
                }

                String methodName = method.getName();
                if("getMetadataMap".equals(methodName)){
                    mergeMetadata(container, request.getMetadataMap());
                    continue;
                }
                
                
                String baseSql = "UPDATE %s SET %s=? WHERE %s = ?";
                Patchable p = method.getAnnotation(Patchable.class);
                String sql = String.format(baseSql, table, p.column(), table);

                try(PreparedStatement stmt = getConnection().prepareStatement(sql)) {
                    stmt.setObject(1, patchedValue);
                    stmt.setLong(2, container.getPk());
                    stmt.executeUpdate();
                }
            }
        }
    }

    protected DirectoryStream<DatacatNode> getChildrenStreamInternal(DatacatRecord parent,
            DatasetView viewPrefetch) throws SQLException, IOException{
        Long parentPk = parent.getPk();
        String parentPath = parent.getPath();
        String sql = getChildrenSql(parent, viewPrefetch);
        
        final PreparedStatement stmt = getConnection().prepareStatement(sql);
        final PreparedStatement prefetchVer;
        final PreparedStatement prefetchLoc;
        stmt.setLong(1, parentPk);

        if(viewPrefetch != null){
            prefetchVer = getConnection()
                    .prepareStatement(getVersionsSql(VersionParent.CONTAINER, viewPrefetch));
            prefetchVer.setLong(1, parentPk);
            if(!viewPrefetch.isCurrent()){
                prefetchVer.setInt(2, viewPrefetch.getVersionId());
            }
            if(!viewPrefetch.zeroSites()){
                prefetchLoc = getConnection()
                        .prepareStatement(getLocationsSql(VersionParent.CONTAINER, viewPrefetch));
                prefetchLoc.setLong(1, parentPk);
                if(!viewPrefetch.isCurrent()){
                    prefetchLoc.setInt(2, viewPrefetch.getVersionId());
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
        DirectoryStream<DatacatNode> stream = new DirectoryStream<DatacatNode>() {
            Iterator<DatacatNode> iter = null;

            @Override
            public Iterator<DatacatNode> iterator(){
                if(iter == null){
                    iter = new Iterator<DatacatNode>() {

                        boolean beforeStart = true;
                        boolean wasOkay = false;
                        boolean consumed = false;

                        @Override
                        public boolean hasNext(){
                            try {
                                return checkNext();
                            } catch(SQLException | NoSuchElementException ex) {
                                return false;
                            }
                        }

                        private boolean checkNext() throws SQLException{
                            if(beforeStart || (wasOkay && consumed)){
                                consumed = false;
                                beforeStart = false;
                                wasOkay = rs.next();
                            }
                            return wasOkay;
                        }

                        @Override
                        public DatacatNode next(){
                            if(!hasNext()){
                                throw new NoSuchElementException();
                            }
                            try {
                                DatacatObject.Builder builder = getBuilder(rs, parentPath);
                                if(builder instanceof Dataset.Builder){
                                    checkResultSet((Dataset.Builder) builder, rsVer, rsLoc);
                                }
                                consumed = true;
                                ContainerDAOMySQL.this.completeObject(builder);
                                return builder.build();
                            } catch(SQLException ex) {
                                throw new RuntimeException(ex);
                            }
                        }

                        @Override
                        public void remove(){
                            throw new UnsupportedOperationException();
                        }

                    };
                }
                return iter;
            }

            @Override
            public void close() throws IOException{
                try {
                    stmt.close();
                    if(prefetchVer != null){
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

    private void checkResultSet(Dataset.Builder dsBuilder, ResultSet dsVer, ResultSet dsLoc) throws SQLException{
        completeDataset(dsBuilder);
        long dsPk = dsBuilder.pk;
        if(dsVer == null || dsVer.isClosed()){
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
        List<DatasetViewInfo> views = new ArrayList<>();
        DatasetVersion.Builder builder = new DatasetVersion.Builder();

        long verPk = dsVer.getLong("datasetversion");

        while(!dsVer.isClosed() && dsVer.getLong("dataset") == dsPk && dsVer.getLong("datasetversion") == verPk){
            HashMap<String, Object> metadata = new HashMap<>();
            List<DatasetLocationModel> locations = new ArrayList<>();
            // Update
            builder.pk(verPk);
            builder.parentPk(dsPk);
            builder.versionId(dsVer.getInt("versionid"));
            builder.datasetSource(dsVer.getString("datasetSource"));
            builder.latest(dsVer.getBoolean("isLatest"));
            while(!dsVer.isClosed() && dsVer.getLong("dataset") == dsPk && dsVer.getLong("datasetversion") == verPk){
                // Process all metadata entries first, 1 or more rows per version
                SqlBaseDAO.processMetadata(dsVer, metadata);
                if(!dsVer.next()){
                    dsVer.close();
                }
            }
            /* After we've processed all result rows for each version, (mostly metadata)
             process all locations for this version */
            while(dsLoc != null && !dsLoc.isClosed() && dsLoc.getLong("datasetversion") == verPk){
                // Assume one location per row. Row could be null (LEFT OUTER JOIN)
                if(dsLoc.getString("datasetsite") != null){
                    SqlBaseDAO.processLocation(dsLoc, builder.pk, locations);
                }
                if(!dsLoc.next()){
                    dsLoc.close();
                }
            }
            builder.metadata(metadata);
            views.add(new DatasetViewInfo(builder.build(), locations));
        }
        if(views.size() == 1){
            dsBuilder.view(views.get(0));
        }
        // TODO: Support multiple versions?
    }

    protected String getChildrenSql(DatacatRecord parent, DatasetView viewPrefetch){
        String sql;
        if (parent.getType() == RecordType.GROUP){
            sql = "SELECT objects.type, objects.pk, objects.name, objects.parent, objects.acl FROM ( "
                    + "    SELECT   'D' type, dataset pk, datasetName name, datasetgroup parent, acl "
                    + "      FROM VerDataset "
                    + ") objects "
                    + "  WHERE objects.parent = ? "
                    + "  ORDER BY objects.name";
        } else { // RecordType.FOLDER
            sql = "SELECT objects.type, objects.pk, objects.name, objects.parent, objects.acl FROM ( "
                    + "    SELECT 'F' type, datasetlogicalfolder pk, name, parent, acl "
                    + "      FROM DatasetLogicalFolder "
                    + "  UNION ALL "
                    + "    SELECT 'G' type, datasetGroup pk, name, datasetLogicalFolder parent, acl "
                    + "      FROM DatasetGroup "
                    + (viewPrefetch != null ? "  UNION ALL "
                    + "    SELECT   'D' type, dataset pk, datasetName name, "
                    + "      CASE WHEN datasetlogicalfolder is not null "
                    + "        THEN datasetlogicalfolder else datasetgroup END parent, acl "
                    + "      FROM VerDataset " : " ")
                    + ") objects "
                    + "  WHERE objects.parent = ? "
                    + "  ORDER BY objects.name";
        }
        return sql;
    }

}
