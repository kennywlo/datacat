package org.srs.datacat.dao.sql;

import com.google.common.base.Optional;
import org.srs.datacat.dao.sql.search.SearchUtils;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.*;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.shared.*;
import org.srs.vfs.AbstractFsProvider.AfsException;
import org.srs.vfs.PathUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author bvan
 */
public class SqlBaseDAO implements org.srs.datacat.dao.BaseDAO {

    private final Connection conn;
    private ReentrantLock lock;
    private final SqlDAOFactory.Locker locker;

    public SqlBaseDAO(Connection conn, SqlDAOFactory.Locker locker) {
        this.conn = conn;
        this.locker = locker;
    }

    public SqlDAOFactory.Locker getLocker() {
        return locker;
    }

    @Override
    public void lock(Path lockPath) throws IOException {
        this.lock = locker.prepareLease(lockPath);
    }

    private void unlock() {
        if (lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ex) {
            throw new IOException("Error closing data source", ex);
        } finally {
            unlock();
        }
    }

    @Override
    public void commit() throws IOException {
        try {
            conn.commit();
        } catch (SQLException ex) {
            throw new IOException("Error committing changes", ex);
        } finally {
            unlock();
        }
    }

    protected Connection getConnection() {
        return this.conn;
    }

    protected void delete1(String sql, Object o) throws SQLException {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setObject(1, o);
            stmt.executeUpdate();
            // timing-sensitive
            this.commit();
        } catch (IOException ex) {
            throw new SQLException(ex);
        } catch (SQLException ex){
            getConnection().rollback();
            if (ex.getMessage().toLowerCase().contains("deadlock")){
                // this should only happen in MySQL testing
                System.out.println("delete1(): Deadlock detected");
                try {
                    Thread.sleep((int)(Math.random()*1000));
                    delete1(sql, o);
                } catch (InterruptedException e){
                    throw new SQLException(e);
                }
            } else{
                throw new SQLException(ex);
            }
        }
    }

    @Override
    public DatacatNode getObjectInParent(DatacatRecord parent, String name) throws IOException, NoSuchFileException {
        return getDatacatObject(parent, name);
    }

    private DatacatNode getDatacatObject(DatacatRecord parent, String name) throws IOException, NoSuchFileException {
        try {
            return getChild(parent, name);
        } catch (SQLException ex) {
            throw new IOException("Unknown exception occurred in the database", ex);
        }
    }

    private DatacatNode getChild(DatacatRecord parent, String name) throws SQLException, NoSuchFileException {
        String parentPath = parent != null ? parent.getPath() : null;
        String nameParam = null;

        String childPath = parent != null ? PathUtils.resolve(parent.getPath(), name) : name;
        String parentClause;
        if (parentPath == null || "/".equals(name)) {
            parentClause = " is null ";
        } else {
            nameParam = name;
            parentClause = " = ? and name = ?";
        }

        String sql = getChildSql(parentClause);

        DatacatObject.Builder builder;
        Long pk = parent != null ? parent.getPk() : null;
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            if (nameParam != null) {
                stmt.setLong(1, pk);
                stmt.setString(2, nameParam);
            }
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                String msg = String.format("Unable to resolve %s in parent %s", childPath, parent);
                throw new NoSuchFileException(msg);
            }
            builder = getBuilder(rs, parentPath);
        }
        completeObject(builder);
        return builder.build();
    }

    protected void completeObject(org.srs.datacat.shared.DatacatObject.Builder builder) throws SQLException {
        if (builder instanceof Dataset.Builder) {
            completeDataset((Dataset.Builder) builder);
        } else if (builder instanceof DatasetGroup.Builder) {
            completeContainer((DatasetGroup.Builder) builder,
                    "select description from DatasetGroup where datasetgroup = ?");
            setContainerMetadata(builder);
        } else if (builder instanceof LogicalFolder.Builder) {
            completeContainer((LogicalFolder.Builder) builder,
                    "select description from DatasetLogicalFolder where datasetlogicalfolder = ?");
            setContainerMetadata(builder);
        }
    }

    protected void completeDataset(Dataset.Builder builder) throws SQLException {
        String sql = "select vd.datasetfileformat, "
                + "vd.datasetdatatype, vd.latestversion, "
                + "vd.registered created "
                + "from VerDataset vd "
                + "where vd.dataset = ? ";

        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, builder.pk);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            builder.fileFormat(rs.getString("datasetfileformat"))
                    .dataType(rs.getString("datasetdatatype"))
                    .created(rs.getTimestamp("created"));
        }
    }

    private void completeContainer(DatasetContainerBuilder builder, String sql) throws SQLException {
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, builder.pk);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            builder.description(rs.getString("description"));
        }
    }

    protected void setVersionMetadata(DatasetVersion.Builder builder) throws SQLException {
        String sql = getVersionMetadataSql();
        HashMap<String, Object> metadata = new HashMap<>();
        Long pk = builder.pk;
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, pk);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                processMetadata(rs, metadata);
            }
        }
        // fetch the dependency info
        Map<String, Object> dependents = SearchUtils.getDependents(getConnection(), "dependency",
            builder.pk, "*");
        if (!dependents.isEmpty()) {
            metadata.putAll(dependents);
            if (!metadata.containsKey("dependencyName")) {
                metadata.put("dependencyName", builder.path);
            }
        }
        if (!metadata.isEmpty()) {
            builder.metadata(metadata);
        }
    }

    private void setContainerMetadata(org.srs.datacat.shared.DatacatObject.Builder builder) throws SQLException {
        String tableType = null;
        Long pk = builder.pk;
        if (builder instanceof LogicalFolder.Builder) {
            tableType = "LogicalFolder";
        } else if (builder instanceof DatasetGroup.Builder) {
            tableType = "DatasetGroup";
        }
        Map<String, Object> metadata = getMetadata(pk, tableType, tableType);
        // fetch the dependency info
        if (tableType != null && tableType.equals("DatasetGroup")) {
            Map<String, Object> dependents = SearchUtils.getDependents(getConnection(), "dependencyGroup",
                pk, "*");
            if (dependents.isEmpty()){
                metadata.remove("dependencyName");
            } else {
                metadata.putAll(dependents);
                if (!metadata.containsKey("dependencyName")){
                    metadata.put("dependencyName", builder.path);
                }
            }
        }
        if (!metadata.isEmpty()) {
            builder.metadata(metadata);
        }
    }

    private Map<String, Object> getMetadata(long pk, String tablePrefix, String column) throws SQLException {
        HashMap<String, Object> metadata = new HashMap<>();
        String mdBase = "select Metaname, Metavalue from %sMeta%s where %s = ?";
        String sql = String.format(mdBase, tablePrefix, "String", column);
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, pk);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                metadata.put(rs.getString("metaname"), rs.getString("metavalue"));
            }
        }

        sql = String.format(mdBase, tablePrefix, "Number", column);
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, pk);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                java.math.BigDecimal v = rs.getBigDecimal("metavalue");
                Number n = v.scale() == 0 ? v.toBigIntegerExact() : v;
                metadata.put(rs.getString("metaname"), n);
            }
        }
        sql = String.format(mdBase, tablePrefix, "Timestamp", column);
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setLong(1, pk);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Timestamp t = rs.getTimestamp("metavalue");
                metadata.put(rs.getString("metaname"), t);
            }
        }
        return metadata;
    }

    @Override
    public void delete(DatacatRecord record) throws IOException {
        if (record.getType().isContainer()) {
            doDeleteDirectory(record);
        } else {
            doDeleteDataset(record);
        }
    }

    protected void doDeleteDirectory(DatacatRecord record) throws DirectoryNotEmptyException, IOException {
        if (!record.getType().isContainer()) {
            String msg = "Unable to delete object: Not a Group or Folder" + record.getType();
            throw new IOException(msg);
        }
        SqlContainerDAO dao = new SqlContainerDAO(getConnection(), locker);
        // Verify directory is empty
        try (DirectoryStream ds = dao.getChildrenStream(record, Optional.of(DatasetView.EMPTY))) {
            if (ds.iterator().hasNext()) {
                AfsException.DIRECTORY_NOT_EMPTY.throwError(record.getPath(), "Container not empty");
            }
        }
        dao.deleteContainer(record);
    }

    private void doDeleteDataset(DatacatRecord record) throws IOException {
        if (!(record.getType() == RecordType.DATASET)) {
            throw new IOException("Can only delete Datacat objects");
        }
        SqlDatasetDAO dao = new SqlDatasetDAO(getConnection(), locker);
        dao.deleteDataset(record);
    }

    @Override
    public void mergeMetadata(DatacatRecord record, Map<String, Object> metaData) throws IOException {
        try {
            switch (record.getType()) {
                case DATASETVERSION:
                    mergeDatasetVersionMetadata(record, metaData);
                    break;
                case GROUP:
                    mergeGroupMetadata(record, metaData);
                    break;
                case FOLDER:
                    mergeFolderMetadata(record.getPk(), metaData);
                    break;
                default:
                    String msg = "Unable to add metadata to object type: " + record.getType();
                    throw new IOException(msg);
            }
        } catch (SQLException ex) {
            throw new IOException("Unable to add metadata to object", ex);
        }
    }

    protected void addDatasetVersionMetadata(DatasetVersion dv, Map<String, Object> metaData) throws SQLException {
        Long pk = dv.getPk();
        metaData.put("dependency", pk);
        metaData.put("dependencyName", dv.getPath());
        addDependency(metaData);
        addDatacatObjectMetadata(pk, metaData, "VerDataset", "DatasetVersion");
    }

    protected void addGroupMetadata(DatasetContainer datasetGroup, Map<String, Object> metaData) throws SQLException {
        long datasetGroupPk = datasetGroup.getPk();
        metaData.put("dependencyName", datasetGroup.getPath());
        metaData.put("dependencyGroup", datasetGroupPk);
        addDependency(metaData);
        addDatacatObjectMetadata(datasetGroupPk, metaData, "DatasetGroup", "DatasetGroup");
    }

    protected void addFolderMetadata(long logicalFolderPK, Map<String, Object> metaData) throws SQLException {
        addDatacatObjectMetadata(logicalFolderPK, metaData, "LogicalFolder", "LogicalFolder");
    }

    protected void mergeDatasetVersionMetadata(DatacatRecord ds, Map<String, Object> metaData) throws SQLException {
        long pK = ds.getPk();
        metaData.put("dependency", pK);
        metaData.put("dependencyName", ds.getPath());
        mergeDependencyMetadata(metaData);
        mergeDatacatObjectMetadata(pK, metaData, "VerDataset", "DatasetVersion");
    }

    private void mergeGroupMetadata(DatacatRecord datasetGroup, Map<String, Object> metaData) throws SQLException {
        long datasetGroupPk = datasetGroup.getPk();
        metaData.put("dependencyName", datasetGroup.getPath());
        metaData.put("dependencyGroup", datasetGroupPk);
        mergeDependencyMetadata(metaData);
        mergeDatacatObjectMetadata(datasetGroupPk, metaData, "DatasetGroup", "DatasetGroup");
    }

    private void mergeFolderMetadata(long logicalFolderPK, Map<String, Object> metaData) throws SQLException {
        mergeDatacatObjectMetadata(logicalFolderPK, metaData, "LogicalFolder", "LogicalFolder");
    }

    protected void mergeDependencyMetadata(Map<String, Object> metaData)
        throws SQLException {
        String depContainer;
        if (metaData.containsKey("dependency")){
            depContainer = "dependency";
        } else if (metaData.containsKey("dependencyGroup")){
            depContainer = "dependencyGroup";
        } else{
            throw new SQLException("Missing dependency container");
        }
        // first, let's remove the existing dependency info
        if (metaData.containsKey("dependents") || metaData.containsKey("dependentGroups")) {
            String type = (String) metaData.get("dependentType");
            SearchUtils.deleteDependentsByType(getConnection(), depContainer,
                    (long) metaData.get(depContainer), type);
        }
        if (metaData.get("predecessor.dataset") != null){
            SearchUtils.deleteDependentsByType(getConnection(), depContainer,
                (long) metaData.get(depContainer), "predecessor");
        }
        if (metaData.get("predecessor.group") != null){
            SearchUtils.deleteDependentsByType(getConnection(), depContainer,
                (long) metaData.get(depContainer), "predecessor");
        }
        if (metaData.get("successor.dataset") != null){
            SearchUtils.deleteDependentsByType(getConnection(), depContainer,
                (long) metaData.get(depContainer), "successor");
        }
        if (metaData.get("successor.group") != null){
            SearchUtils.deleteDependentsByType(getConnection(), depContainer,
                (long) metaData.get(depContainer), "successor");
        }
        // for timing
        getConnection().commit();

        // now add the updated dependency info
        addDependency(metaData);
    }

    protected void addDependency(Map<String, Object> metaData) throws SQLException {
        if (!(metaData instanceof HashMap)) {
            metaData = new HashMap<>(metaData);
        }

        Long dependency = (Long)metaData.remove("dependency");
        Long dependencyGroup = (Long)metaData.remove("dependencyGroup");
        // keep dependencyName to store as metadata field of the Dependency container
        String dependencyName = (String)metaData.get("dependencyName");

        // process dependents in normalized form
        String pd = (String) metaData.remove("predecessor.dataset");
        if (pd != null){
            saveDependents(dependencyName, dependency, dependencyGroup, pd, null, "predecessor");
        }
        String pg = (String) metaData.remove("predecessor.group");
        if (pg != null){
            saveDependents(dependencyName, dependency, dependencyGroup, null, pg, "predecessor");
        }
        String sd = (String) metaData.remove("successor.dataset");
        if (sd != null){
            saveDependents(dependencyName, dependency, dependencyGroup, sd, null, "successor");
        }
        String sg = (String) metaData.remove("successor.group");
        if (sg != null){
            saveDependents(dependencyName, dependency, dependencyGroup, null, sg, "successor");
        }
        // process dependents in regular form
        String dependents = (String)metaData.remove("dependents");
        String dependentGroups = (String)metaData.remove("dependentGroups");
        String dependentType = (String)metaData.remove("dependentType");
        saveDependents(dependencyName, dependency, dependencyGroup, dependents, dependentGroups, dependentType);

        // for timing below
        getConnection().commit();

        // retrieve and return all dependents in normalized form
        if (SearchUtils.getDependency(getConnection(), dependencyName).isEmpty()){
            // no dependents, hence not a dependency container
            metaData.remove("dependencyName");
        }
    }

    protected void saveDependents(String dependencyName, Long dependency, Long dependencyGroup, String dependents,
                                  String dependentGroups, String dependentType) throws SQLException {
        // add dataset dependents
        if (dependents != null && !dependents.isEmpty()) {
            String dependencySql = "insert into DatasetDependency (Dependency, DependencyGroup, DependencyName, " +
                "Dependent, DependentType)" + " values (?, ?, ?, ?, ?)";
            String[] dependentList = dependents.replaceAll("[\\[\\](){}]", "").split("[ ,]+");
            try (PreparedStatement stmt = getConnection().prepareStatement(dependencySql)){
                // process each dependent from the list
                for (String d : dependentList) {
                    long dependent = Long.parseLong(d);
                    if (dependency != null) {
                        stmt.setLong(1, dependency);
                    } else {
                        stmt.setNull(1, Types.BIGINT);
                    }
                    if (dependencyGroup != null) {
                        stmt.setLong(2, dependencyGroup);
                    } else {
                        stmt.setNull(2, Types.BIGINT);
                    }
                    stmt.setString(3, dependencyName);
                    stmt.setLong(4, dependent);
                    stmt.setString(5, dependentType);
                    try {
                        stmt.executeUpdate();
                    } catch (SQLException ex){
                        getConnection().rollback();
                        if (ex.getMessage().toLowerCase().contains("deadlock")){
                            // should only happen in MySQL testing
                            System.out.println("saveDependents(ds): Deadlock detected...retrying.");
                            try {
                                Thread.sleep((int) (Math.random() * 1000));
                            } catch (InterruptedException e){
                                throw new SQLException(e);
                            }
                            // retry
                            saveDependents(dependencyName, dependency, dependencyGroup, dependents, dependentGroups,
                                dependentType);
                        } else {
                            throw new SQLException(ex);
                        }
                    }
                }
            }
        }
        // add group dependents
        if (dependentGroups != null && !dependentGroups.isEmpty()) {
            String dependencySql = "insert into DatasetDependency (Dependency, DependencyGroup, DependencyName, " +
                "DependentGroup, DependentType)" + " values (?, ?, ?, ?, ?)";
            String[] dependentList = dependentGroups.replaceAll("[\\[\\](){}]", "").split("[ ,]+");
            try (PreparedStatement stmt = getConnection().prepareStatement(dependencySql)){
                // store each dependent info from the list
                for (String d : dependentList) {
                    long dependentGroup = Long.parseLong(d);
                    if (dependency != null) {
                        stmt.setLong(1, dependency);
                    } else {
                        stmt.setNull(1, Types.BIGINT);
                    }
                    if (dependencyGroup != null) {
                        stmt.setLong(2, dependencyGroup);
                    } else {
                        stmt.setNull(2, Types.BIGINT);
                    }
                    stmt.setString(3, dependencyName);
                    stmt.setLong(4, dependentGroup);
                    stmt.setString(5, dependentType);
                    try {
                        stmt.executeUpdate();
                    } catch (SQLException ex){
                        getConnection().rollback();
                        if (ex.getMessage().toLowerCase().contains("deadlock")){
                            // should only happen in MySQL testing
                            System.out.println("saveDependents(grp): Deadlock detected...retrying.");
                            try {
                                Thread.sleep((int) (Math.random() * 1000));
                            } catch (InterruptedException e){
                                throw new SQLException(e);
                            }
                            // retry
                            saveDependents(dependencyName, dependency, dependencyGroup, dependents, dependentGroups,
                                dependentType);
                        } else {
                            throw new SQLException(ex);
                        }
                    }
                }
            }
        }
    }
//    protected void deleteDatasetVersionMetadata(Long pk, Set<String> metaDataKeys) throws SQLException{
//        deleteDatacatObjectMetadata(pk, metaDataKeys, "VerDataset", "DatasetVersion");
//    }
//
//    protected void deleteGroupMetadata(long datasetGroupPK, Set<String> metaDataKeys) throws SQLException{
//        deleteDatacatObjectMetadata(datasetGroupPK, metaDataKeys, "DatasetGroup", "DatasetGroup");
//    }
//
//    protected void deleteFolderMetadata(long logicalFolderPK, Set<String> metaDataKeys) throws SQLException{
//        deleteDatacatObjectMetadata(logicalFolderPK, metaDataKeys, "LogicalFolder", "LogicalFolder");
//    }

    private void addDatacatObjectMetadata(long objectPK, Map<String, Object> metaData, String tablePrefix,
                                          String column) throws SQLException {
        if (metaData == null) {
            return;
        }
        if (!(metaData instanceof HashMap)) {
            metaData = new HashMap<>(metaData);
        }
        final String metaSql = "insert into %sMeta%s (%s,MetaName,MetaValue) values (?,?,?)";
        String metaStringSql = String.format(metaSql, tablePrefix, "String", column);
        String metaNumberSql = String.format(metaSql, tablePrefix, "Number", column);
        String metaTimestampSql = String.format(metaSql, tablePrefix, "Timestamp", column);
        PreparedStatement stmt;

        try (PreparedStatement stmtMetaString = getConnection().prepareStatement(metaStringSql);
             PreparedStatement stmtMetaNumber = getConnection().prepareStatement(metaNumberSql);
             PreparedStatement stmtMetaTimestamp = getConnection().prepareStatement(metaTimestampSql)) {
            for (Map.Entry<String, Object> stringObjectEntry : metaData.entrySet()) {
                String metaName = stringObjectEntry.getKey();
                Object metaValue = stringObjectEntry.getValue();

                // Determine MetaData Object type and insert it into the appropriate table:
                if (metaValue instanceof Timestamp) {
                    stmt = stmtMetaTimestamp;
                    stmt.setTimestamp(3, (Timestamp) metaValue);
                } else if (metaValue instanceof Number) {
                    stmt = stmtMetaNumber;
                    stmt.setObject(3, metaValue);
                } else { // all others stored as String
                    stmt = stmtMetaString;
                    stmt.setString(3, metaValue.toString());
                }

                stmt.setLong(1, objectPK);
                stmt.setString(2, metaName);
                stmt.executeUpdate();
            }
        } catch (SQLException ex){
            getConnection().rollback();
            if (ex.getMessage().toLowerCase().contains("deadlock")){
                // this should only happen in MySQL testing
                System.out.println("addDatacatObjectMetadata(): Deadlock detected...retrying.");
                try {
                    Thread.sleep((int)(Math.random()*1000));
                    addDatacatObjectMetadata(objectPK, metaData, tablePrefix, column);
                } catch (InterruptedException e){
                    throw new SQLException(e);
                }
            } else{
                throw new SQLException(ex);
            }
        }

    }

    private void mergeDatacatObjectMetadata(long objectPK, Map<String, Object> metaData, String tablePrefix,
                                            String column) throws SQLException {
        Map<String, Object> insertMetaData = new HashMap<>();
        Set<String> deleteMetadata = new HashSet<>();
        if (metaData == null) {
            return;
        }
        if (!(metaData instanceof HashMap)) {
            metaData = new HashMap<>(metaData);
        }
        final String metaSql = "UPDATE %sMeta%s SET MetaValue = ? WHERE MetaName= ? and %s = ?";
        String metaStringSql = String.format(metaSql, tablePrefix, "String", column);
        String metaNumberSql = String.format(metaSql, tablePrefix, "Number", column);
        String metaTimestampSql = String.format(metaSql, tablePrefix, "Timestamp", column);
        PreparedStatement stmt;

        try (PreparedStatement stmtMetaString = getConnection().prepareStatement(metaStringSql);
             PreparedStatement stmtMetaNumber = getConnection().prepareStatement(metaNumberSql);
             PreparedStatement stmtMetaTimestamp = getConnection().prepareStatement(metaTimestampSql)) {
            for (Map.Entry<String, Object> stringObjectEntry : metaData.entrySet()) {
                String metaName = stringObjectEntry.getKey();
                Object metaValue = stringObjectEntry.getValue();

                if (metaValue == null) {
                    deleteMetadata.add(metaName);
                    continue;
                }

                // Determine MetaData Object type and insert it into the appropriate table:
                if (metaValue instanceof Timestamp) {
                    stmt = stmtMetaTimestamp;
                    stmt.setTimestamp(1, (Timestamp) metaValue);
                } else if (metaValue instanceof Number) {
                    stmt = stmtMetaNumber;
                    stmt.setObject(1, metaValue);
                } else { // all others stored as String
                    stmt = stmtMetaString;
                    stmt.setString(1, metaValue.toString());
                }

                stmt.setLong(3, objectPK);
                stmt.setString(2, metaName);
                int result = stmt.executeUpdate();
                if (result == 0) {
                    insertMetaData.put(metaName, metaValue);
                }
            }
        }
        deleteDatacatObjectMetadata(objectPK, deleteMetadata, tablePrefix, column);
        addDatacatObjectMetadata(objectPK, insertMetaData, tablePrefix, column);
    }

    private void deleteDatacatObjectMetadata(long objectPK, Set<String> metaDataKeys, String tablePrefix,
                                             String column) throws SQLException {
        if (metaDataKeys == null) {
            return;
        }
        final String metaSql = "DELETE FROM %sMeta%s WHERE MetaName= ? and %s = ?";
        String metaStringSql = String.format(metaSql, tablePrefix, "String", column);
        String metaNumberSql = String.format(metaSql, tablePrefix, "Number", column);
        String metaTimestampSql = String.format(metaSql, tablePrefix, "Timestamp", column);
        PreparedStatement stmt;

        try (PreparedStatement stmtMetaString = getConnection().prepareStatement(metaStringSql);
             PreparedStatement stmtMetaNumber = getConnection().prepareStatement(metaNumberSql);
             PreparedStatement stmtMetaTimestamp = getConnection().prepareStatement(metaTimestampSql)) {
            Iterator<String> i = metaDataKeys.iterator();

            Map<String, Object> existingMetadata = getMetadata(objectPK, tablePrefix, column);
            while (i.hasNext()) {
                String metaName = i.next();
                Object metaValue = existingMetadata.get(metaName);

                // Determine MetaData Object type and insert it into the appropriate table:
                if (metaValue instanceof Timestamp) {
                    stmt = stmtMetaTimestamp;
                } else if (metaValue instanceof Number) {
                    stmt = stmtMetaNumber;
                } else { // all others stored as String
                    stmt = stmtMetaString;
                }

                stmt.setString(1, metaName);
                stmt.setLong(2, objectPK);
                stmt.executeUpdate();
            }
        }
    }

    protected static RecordType getType(String typeChar) {
        switch (typeChar) {
            case "F":
                return RecordType.FOLDER;
            case "G":
                return RecordType.GROUP;
            case "D":
                return RecordType.DATASET;
            default:
                return null;
        }
    }

    protected static DatacatObject.Builder getBuilder(ResultSet rs, String parentPath) throws SQLException {
        RecordType type = getType(rs.getString("type"));
        DatacatObject.Builder o;
        assert type != null;
        switch (type) {
            case DATASET:
                o = new Dataset.Builder();
                break;
            case FOLDER:
                o = new LogicalFolder.Builder();
                break;
            case GROUP:
                o = new DatasetGroup.Builder();
                break;
            default:
                o = new DatacatObject.Builder();
        }
        String name = rs.getString("name");
        o.pk(rs.getLong("pk"))
                .parentPk(rs.getLong("parent"))
                .name(name)
                .acl(rs.getString("acl"));
        if (parentPath != null && !parentPath.isEmpty()) {
            o.path(PathUtils.resolve(parentPath, name));
        } else {
            o.path("/");
        }
        return o;
    }

    protected static void processMetadata(ResultSet rs, HashMap<String, Object> metadata) throws SQLException {
        String mdType = rs.getString("mdtype");
        if (mdType == null) {
            return;
        }
        switch (rs.getString("mdtype")) {
            case "N":
                Number n;
                java.math.BigDecimal v = rs.getBigDecimal("metanumber");
                n = v.scale() == 0 ? v.toBigIntegerExact() : v;
                metadata.put(rs.getString("metaname"), (Number) n);
                return;
            case "S":
                metadata.put(rs.getString("metaname"), rs.getString("metastring"));
                return;
            case "T":
                metadata.put(rs.getString("metaname"), rs.getTimestamp("metatimestamp"));
            default:
        }
    }

    protected static void processLocation(ResultSet rs, Long versionPk,
                                          List<DatasetLocationModel> locations) throws SQLException {
        DatasetLocation.Builder builder = new DatasetLocation.Builder();
        builder.pk(rs.getLong("datasetlocation"));
        builder.parentPk(versionPk);
        builder.site(rs.getString("datasetsite"));
        builder.resource(rs.getString("path"));
        builder.runMin(rs.getLong("runmin"));
        builder.runMax(rs.getLong("runmax"));
        builder.eventCount(rs.getLong("numberevents"));
        builder.size(rs.getLong("filesizebytes"));
        BigDecimal bd = rs.getBigDecimal("checksum");
        if (bd != null) {
            builder.checksum(bd.unscaledValue().toString(16));
        }
        builder.modified(rs.getTimestamp("lastmodified"));
        builder.scanned(rs.getTimestamp("lastscanned"));
        builder.scanStatus(rs.getString("scanstatus"));
        builder.created(rs.getTimestamp("registered"));
        builder.master(rs.getBoolean("isMaster"));
        locations.add(builder.build());
    }

    @Override
    public <T extends DatacatNode> T createNode(DatacatRecord parent, String path,
                                                T request) throws IOException, FileSystemException {
        if (request instanceof Dataset) {
            SqlDatasetDAO dao = new SqlDatasetDAO(getConnection(), locker);
            return (T) dao.createDatasetNode(parent, path, (Dataset) request);
        }
        if (request instanceof DatasetContainer) {
            // It should be a container
            SqlContainerDAO dao = new SqlContainerDAO(getConnection(), locker);
            return (T) dao.createContainer(parent, path, (DatasetContainer) request);
        }
        throw new IOException(new IllegalArgumentException("Unable to process request object"));
    }

    @Override
    public void setAcl(DatacatRecord record, String acl) throws IOException {
        try {
            setAclInternal(record, acl);
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
    }

    private void setAclInternal(DatacatRecord record, String acl) throws SQLException {
        String sql = "UPDATE %s SET ACL = ? WHERE %s = ?";
        String tableType = "DatasetLogicalFolder";
        if (record instanceof DatasetGroup.Builder) {
            tableType = "DatasetGroup";
        }
        sql = String.format(sql, tableType, tableType);
        try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setString(1, acl);
            stmt.setLong(2, record.getPk());
            stmt.executeUpdate();
        }
    }

    protected enum VersionParent {
        DATASET,
        CONTAINER;
    }

    protected String getVersionsSql(VersionParent condition, DatasetView view) {
        String queryCondition = "";
        switch (condition) {
            case DATASET:
                queryCondition = "vd.dataset = ? ";
                break;
            case CONTAINER:
                queryCondition = "vd.parent = ? ";
                break;
            default:
                break;
        }

        String datasetSqlWithMetadata =
                "WITH Dataset (dataset, parent, name, latestversion) as ("
                        + "  SELECT ds.dataset, CASE WHEN ds.datasetlogicalfolder is not null "
                        + "      THEN ds.datasetlogicalfolder else ds.datasetgroup END parent, "
                        + "      ds.datasetname name, ds.latestversion "
                        + "  FROM VerDataset ds"
                        + "), "
                        + "DatasetVersions (dataset, datasetversion, versionid, datasetsource, islatest) AS ( "
                        + "  select vd.dataset, dsv.datasetversion, dsv.versionid, dsv.datasetsource, "
                        + "        CASE WHEN vd.latestversion = dsv.datasetversion THEN 1 ELSE 0 END isLatest "
                        + "        FROM Dataset vd "
                        + "        JOIN DatasetVersion dsv on (vd.latestversion = dsv.datasetversion) "
                        + "        WHERE " + queryCondition
                        + "            and " + versionString(view)
                        + "       ORDER BY vd.name, dsv.versionid desc "
                        + ") "
                        + "SELECT dsv.dataset, dsv.datasetversion, dsv.versionid, dsv.datasetsource, dsv.islatest,  "
                        + "     md.mdtype, md.metaname, md.metastring, md.metanumber, md.metatimestamp "
                        + "FROM DatasetVersions dsv "
                        + " JOIN "
                        + " ( SELECT mn.datasetversion, 'N' mdtype, mn.metaname, "
                        + "         null metastring, mn.metavalue metanumber, null metatimestamp   "
                        + "     FROM VerDatasetMetaNumber mn "
                        + "   UNION ALL  "
                        + "   SELECT ms.datasetversion, 'S' mdtype, ms.metaname, "
                        + "         ms.metavalue metastring, null metanumber, null metatimestamp   "
                        + "     FROM VerDatasetMetaString ms "
                        + "   UNION ALL  "
                        + "   SELECT mt.datasetversion, 'T' mdtype, mt.metaname, "
                        + "         null metastring, null metanumber, mt.metavalue metatimestamp   "
                        + "     FROM VerDatasetMetaTimestamp mt "
                        + "  ) md on (md.datasetversion = dsv.datasetversion)";
        return datasetSqlWithMetadata;
    }

    protected String getLocationsSql(VersionParent condition, DatasetView view) {
        String queryCondition = "";
        switch (condition) {
            case DATASET:
                queryCondition = "vd.dataset = ? ";
                break;
            case CONTAINER:
                queryCondition = "vd.datasetLogicalFolder = ? ";
                break;
            default:
                break;
        }
        String datasetSqlLocations
            = "WITH Dataset (dataset, parent, name, latestversion) as ("
                + "  SELECT ds.dataset, CASE WHEN ds.datasetlogicalfolder is not null "
                + "      THEN ds.datasetlogicalfolder else ds.datasetgroup END parent, "
                + "      ds.datasetname name, ds.latestversion "
                + "  FROM VerDataset ds "
                + ")"
                + "select vd.dataset, dsv.datasetversion,  "
                + "    vdl.datasetlocation, vdl.datasetsite, vdl.path, vdl.runmin, vdl.runmax,   "
                + "    vdl.numberevents, vdl.filesizebytes, vdl.checksum, vdl.lastmodified,   "
                + "    vdl.lastscanned, vdl.scanstatus, vdl.registered,   "
                + "    CASE WHEN dsv.masterlocation = vdl.datasetlocation THEN 1 ELSE 0 END isMaster   "
                + "  FROM Dataset vd   "
                + "  JOIN DatasetVersion dsv on (vd.latestversion = dsv.datasetversion)   "
                + "  JOIN VerDatasetLocation vdl on (dsv.datasetversion = vdl.datasetversion)  "
                + "  WHERE " + queryCondition
                + "            and " + versionString(view)
                + "  ORDER BY vd.name, dsv.versionid desc, vdl.registered";
        return datasetSqlLocations;
    }

    protected String getChildSql(String parentClause) {
        String sql = String.format("WITH OBJECTS (type, pk, name, parent, acl) AS ( "
                + "    SELECT 'F', datasetlogicalfolder, name, parent, acl "
                + "      FROM DatasetLogicalFolder "
                + "  UNION ALL "
                + "    SELECT 'G', datasetGroup, name, datasetLogicalFolder, acl "
                + "      FROM DatasetGroup "
                + "  UNION ALL "
                + "    SELECT 'D', dataset, datasetName, "
                + "      CASE WHEN datasetlogicalfolder is not null "
                + "         THEN datasetlogicalfolder else datasetgroup END, acl "
                + "      FROM VerDataset "
                + ") "
                + "SELECT type, pk, name, parent, acl FROM OBJECTS "
                + "  WHERE parent %s "
                + "  ORDER BY name", parentClause);
        return sql;
    }

    protected String versionString(DatasetView view) {
        return view.isCurrent() ? " dsv.datasetversion = vd.latestversion " : " dsv.versionid = ? ";
    }

    protected String getVersionMetadataSql() {
        String sql =
            "WITH DSV (dsv) AS ( "
                + "  SELECT ? FROM DUAL "
                + ") "
                + "SELECT type, metaname, metastring, metanumber, metatimestamp FROM  "
                + " ( SELECT 'N' mdtype, mn.metaname, null metastring, mn.metavalue metanumber, null metatimestamp "
                + "     FROM VerDatasetMetaNumber mn where mn.DatasetVersion = (SELECT dsv FROM DSV) "
                + "   UNION ALL "
                + "   SELECT 'S' mdtype, ms.metaname, ms.metavalue metastring, null metanumber, null metatimestamp "
                + "     FROM VerDatasetMetaString ms where ms.DatasetVersion = (SELECT dsv FROM DSV) "
                + "   UNION ALL "
                + "   SELECT 'T' mdtype, mt.metaname, null metastring, null metanumber, mt.metavalue metatimestamp "
                + "     FROM VerDatasetMetaTimestamp mt where mt.DatasetVersion = (SELECT dsv FROM DSV) "
                + "  )";
        return sql;
    }

}
