package org.srs.datacat.dao.sql.search;

import org.freehep.commons.lang.AST;
import org.srs.datacat.model.*;
import org.srs.datacat.model.container.DatasetContainerBuilder;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.shared.DatasetLocation;
import org.srs.vfs.PathUtils;
import org.zerorm.core.Select;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.sql.*;
import java.util.*;

/**
 *
 * @author bvan
 */
public final class SearchUtils {

    private SearchUtils(){
    }

    public static String getErrorString(AST ast, final String ident){
        if(ast.toString().length() < 32){
            return ast.toString();
        }

        final StringBuilder startOfError = new StringBuilder();
        AST.Visitor errorVisitor = new AST.Visitor() {
            @Override
            public boolean visit(AST.Node n){
                if(n.getLeft() != null && n.getRight() != null){
                    startOfError.append("( ");
                }
                boolean continueVisit = true;
                if(n.getLeft() != null){
                    continueVisit = n.getLeft().accept(this);
                }
                if(continueVisit && n.getValue() != null){
                    boolean isTarget = ident.equals(n.getValue());
                    startOfError.append(isTarget ? "<" : " ")
                            .append(n.getValue().toString())
                            .append(isTarget ? ">" : " ");
                }
                if(continueVisit && n.getRight() != null){
                    continueVisit = n.getRight().accept(this);
                    if(continueVisit && n.getLeft() != null && n.getRight() != null){
                        startOfError.append(" )");
                    }
                }

                return continueVisit;
            }
        };
        ast.getRoot().accept(errorVisitor);
        return startOfError.toString();
    }

    public static DatasetModel datasetFactory(Connection conn, ResultSet rs, ModelProvider modelProvider,
            List<String> includedMetadata) throws SQLException{
        DatasetModel.Builder builder = modelProvider.getDatasetBuilder();

        String name = rs.getString("name");
        builder.pk(rs.getLong("pk"));
        builder.parentPk(rs.getLong("parent"));
        builder.name(name);
        builder.fileFormat(rs.getString("fileformat"));
        builder.dataType(rs.getString("datatype"));
        builder.created(rs.getTimestamp("created"));

        long versionPk = rs.getLong("datasetversion");
        builder.versionPk(versionPk);
        builder.versionId(rs.getInt("versionid"));
        builder.latest(rs.getBoolean("latest"));

        String dependencyName="";
        try {
            builder.path(PathUtils.resolve(rs.getString("containerpath"), name));
        } catch (SQLException ex){
            // if it's a dependency container, set the container path
            dependencyName = SearchUtils.getDependencyPath(conn, versionPk);
            if (!dependencyName.isEmpty()) {
                builder.path(dependencyName);
            }
        }
        ArrayList<DatasetLocationModel> locations = new ArrayList<>();
        HashMap<String, Object> metadata = new HashMap<>();
        boolean foundDependents = false;
        for(String s: includedMetadata){
            if (s.contains("dependency") || s.contains("dependents")){
                if (dependencyName.isEmpty()){
                    dependencyName = SearchUtils.getDependencyPath(conn, versionPk);
                }
                if (!dependencyName.isEmpty()) {
                    String[] deps = s.split("\\.");
                    Map<String, Object> dependents = SearchUtils.getDependents(conn, "dependency",
                        versionPk, deps[1]);
                    metadata.put("dependencyName", dependencyName);
                    metadata.putAll(dependents);
                    foundDependents = true;
                } else if (includedMetadata.contains("dependentSearch")) {
                    // this dataset from query result by pk
                    continue;
                } else {
                    // not a matching dataset, skip
                    if (!rs.next()) {
                        rs.close();
                    }
                    return null;
                }
            } else if (s.contains("dependentSearch")){
                // this dataset from query result by pk
                builder.path(rs.getString("path"));
                continue;
            }
            try {
                Object o = rs.getObject(s);
                if (o != null) {
                    if (o instanceof Number) {
                        BigDecimal v = rs.getBigDecimal(s);
                        o = v.scale() == 0 ? v.toBigIntegerExact() : v;
                    }
                    metadata.put(s, o);
                }
            } catch (SQLException ex) {
                // in case column not found
                continue;
            }
        }

        while(!rs.isClosed() && rs.getInt("datasetversion") == versionPk){
            DatasetLocationModel next = processLocation(rs, modelProvider);
            if(next != null){
                locations.add(next);
            }
            if(!rs.next()){
                rs.close();
            }
        }
        builder.locations(locations);
        if (foundDependents){
            builder.versionMetadata(metadata);
        } else{
            builder.metadata(metadata);
        }
        return builder.build();
    }

    public static DatasetContainer containerFactory(Connection conn, ResultSet rs, ModelProvider modelProvider,
            List<String> includedMetadata) throws SQLException{
        DatasetContainerBuilder builder = modelProvider.getContainerBuilder();

        String name = rs.getString("name");
        builder.pk(rs.getLong("pk"));
        try {
            builder.parentPk(rs.getLong("parent"));
        } catch (SQLException e){
            // just ignore, as groups do not have parents
            builder.parentPk(null);
        }
        builder.name(name);
        builder.path(rs.getString("containerpath"));

        HashMap<String, Object> metadata = new HashMap<>();
        for(String s: includedMetadata){
            if (s.contains("dependency") || s.contains("dependents")){
                String path = SearchUtils.getDependencyGroupPath(conn, rs.getLong("pk"));
                if (path.isEmpty()) {
                    if (includedMetadata.contains("dependentSearch")) {
                        // this container is query result by pk
                        continue;
                    } else {
                        if (!rs.next()) {
                            rs.close();
                        }
                        return null;
                    }
                }
                String[] deps = s.split("\\.");
                Map<String, Object> depmap = SearchUtils.getDependents(conn, "dependencyGroup",
                    rs.getLong("pk"), deps[1]);
                builder.path(path);
                builder.type(RecordType.GROUP);
                metadata.put("dependencyName", path);
                metadata.putAll(depmap);
                continue;
            } else if (s.contains("dependentSearch")){
                // this container is query result by pk
                continue;
            }
            Object o = rs.getObject(s);
            if(o != null){
                if(o instanceof Number){
                    BigDecimal v = rs.getBigDecimal(s);
                    o = v.scale() == 0 ? v.toBigIntegerExact() : v;
                }
                metadata.put(s, o);
            }
        }
        builder.metadata(metadata);
        if(!rs.next()){
            rs.close();
        }
        return builder.build();
    }

    private static DatasetLocationModel processLocation(ResultSet rs,
            ModelProvider modelProvider) throws SQLException{
        DatasetLocation.Builder builder = (DatasetLocation.Builder) modelProvider.
                getLocationBuilder();
        Long pk = rs.getLong("datasetlocation");
        if(rs.wasNull()){
            return null;
        }
        builder.pk(pk);
        builder.site(rs.getString("site"));
        builder.resource(rs.getString("path"));
        builder.runMin(rs.getLong("runmin"));
        builder.runMax(rs.getLong("runmax"));
        builder.eventCount(rs.getLong("eventCount"));
        builder.size(rs.getLong("fileSizeBytes"));
        BigDecimal bd = rs.getBigDecimal("checksum");
        if(bd != null){
            builder.checksum(bd.unscaledValue().toString(16));
        }
        builder.master(rs.getBoolean("master"));
        return builder.build();
    }
    
    public static MetanameContext buildDatasetMetaInfoGlobalContext(Connection conn) throws IOException{
        String sql = "select metaname, ValueType, prefix "
                + "    from DatasetMetaInfo vx "
                + "    left outer join ( "
                + "            select substr(v1.metaname,1,4) prefix,  "
                + "                    count(v1.metaname) prefixcount "
                + "            from  "
                + "            DatasetMetaInfo v1 "
                + "            group by substr(v1.metaname,1,4) "
                + "            having count(v1.metaname) > 5 "
                + "    ) v0 on substr(vx.metaname,1,4) = prefix "
                + "    order by prefix asc ";
        return buildMetaInfoGlobalContext(conn, sql);
    }
    
    public static MetanameContext buildContainerMetaInfoGlobalContext(Connection conn) throws IOException{

        String sql = "select metaname, ValueType, prefix "
                + "    from ContainerMetaInfo vx "
                + "    left outer join ( "
                + "            select substr(v1.metaname,1,4) prefix,  "
                + "                    count(v1.metaname) prefixcount "
                + "            from  "
                + "            ContainerMetaInfo v1 "
                + "            group by substr(v1.metaname,1,4) "
                + "            having count(v1.metaname) > 5 "
                + "    ) v0 on substr(vx.metaname,1,4) = prefix "
                + "    order by prefix asc ";
        return buildMetaInfoGlobalContext(conn, sql);
    }

    public static MetanameContext buildMetaInfoGlobalContext(Connection conn, String sql) throws IOException{

        try(PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();

            MetanameContext dmc = new MetanameContext();
            ArrayList<String> postfixes = new ArrayList<>();
            String lastPrefix = null;
            Class lastType = null;
            while(rs.next()){
                String maybePrefix = rs.getString("prefix");
                String metaname = rs.getString("metaname");
                String valueType = rs.getString("ValueType");
                Class type = String.class;
                switch(valueType){
                    case "S":
                        type = String.class;
                        break;
                    case "N":
                        type = Number.class;
                        break;
                    case "T":
                        type = Timestamp.class;
                        break;
                    default:
                        break;
                }

                if(maybePrefix != null && !maybePrefix.isEmpty()){
                    if(lastPrefix != null && !lastPrefix.equals(maybePrefix)){
                        dmc.add(new MetanameContext.Entry(lastPrefix, postfixes,
                            lastPrefix.length(), lastType));
                        postfixes.clear();
                    }
                    lastPrefix = maybePrefix;
                    lastType = type;
                    postfixes.add(metaname);
                } else {
                    if (lastPrefix != null && !postfixes.isEmpty()){
                        dmc.add(new MetanameContext.Entry(lastPrefix, postfixes,
                            lastPrefix.length(), lastType));
                        lastPrefix = null;
                        lastType= null;
                        postfixes.clear();
                    }
                    dmc.add(new MetanameContext.Entry(metaname, type));
                }
            }
            if (lastPrefix != null && !postfixes.isEmpty()){
                dmc.add(new MetanameContext.Entry(lastPrefix, postfixes,
                    lastPrefix.length(), lastType));
            }
            return dmc;
        } catch(SQLException e) {
            throw new IOException("Error retrieving metadata for search", e);
        }
    }

    public static MetanameContext buildGroupMetanameContext(Connection conn) throws SQLException{
        String sql = "select metaname from DatasetGroupMetaName";
        return buildContainerMetanameContext(conn, sql);
    }

    public static MetanameContext buildFolderMetanameContext(Connection conn) throws SQLException{
        String sql = "select metaname from LogicalFolderMetaName";
        return buildContainerMetanameContext(conn, sql);
    }

    protected static MetanameContext buildContainerMetanameContext(Connection conn, String sql) throws SQLException{
        try(PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();
            MetanameContext mnc = new MetanameContext();
            while(rs.next()){
                mnc.add(new MetanameContext.Entry(rs.getString("metaname")));
            }
            return mnc;
        }
    }

    public static void populateParentTempTable(Connection conn,
            DirectoryStream<DatacatNode> containers) throws SQLException{

        if(conn.getMetaData().getDatabaseProductName().contains("MySQL")){
            String dropSql = "drop temporary table if exists ContainerSearch";
            String tableSql 
                = "create temporary table ContainerSearch ( "
                    + "    DatasetLogicalFolder bigint, "
                    + "    DatasetGroup         bigint, "
                    + "    ContainerPath varchar(500) "
                    + ")";
            try(PreparedStatement stmt = conn.prepareStatement(dropSql)) {
                stmt.execute();
            }
            try(PreparedStatement stmt = conn.prepareStatement(tableSql)) {
                stmt.execute();
            }
        }

        String sql = "INSERT INTO ContainerSearch (DatasetLogicalFolder, DatasetGroup, ContainerPath) VALUES (?,?,?)";
        try(PreparedStatement stmt = conn.prepareStatement(sql)) {
            for(DatacatNode file: containers){
                boolean isGroup = file.getType() == RecordType.GROUP;
                stmt.setNull(isGroup ? 1 : 2, Types.VARCHAR);
                stmt.setLong(isGroup ? 2 : 1, file.getPk());
                stmt.setString(3, file.getPath());
                stmt.executeUpdate();
            }
        }
    }

    /*
    public static void pruneParentTempTable(Connection conn, 
    DirectoryWalker.ContainerVisitor visitor) throws SQLException {
        String sql = 
                "DELETE FROM ContainerSearch "
                + " WHERE (DatasetLogicalFolder is not null AND DatasetLogicalFolder NOT IN (%s)) "
                + " OR (DatasetGroup is not null AND DatasetGroup NOT IN (%s))";
        try (PreparedStatement stmt = conn.prepareStatement( sql )){
            while(visitor.files.peek() != null){
                DatacatRecord file = visitor.files.remove();
                boolean isGroup = file.getType() == RecordType.GROUP;
                stmt.setNull( isGroup ? 1 : 2, Types.VARCHAR);
                stmt.setLong( isGroup ? 2 : 1, file.getPk());
                stmt.setString( 3, file.getPath());
                stmt.executeUpdate();
            }
        }
    }
     */
    public static DirectoryStream<DatasetModel> getResults(final Connection conn,
            final ModelProvider modelProvider,
            final Select sel, final List<String> metadataNames) throws SQLException{
        final PreparedStatement stmt = sel.prepareAndBind(conn);
        final ResultSet rs = stmt.executeQuery();
        if(!rs.next()){
            rs.close();
        }
        DirectoryStream<DatasetModel> stream = new DirectoryStream<DatasetModel>() {
            Iterator<DatasetModel> iter = null;

            @Override
            public Iterator<DatasetModel> iterator(){
                if(iter == null){
                    iter = new Iterator<DatasetModel>() {
                        private DatasetModel ds = null;

                        @Override
                        public boolean hasNext(){
                            try {
                                while(ds == null){
                                    if(rs.isClosed()){
                                        return false;
                                    }
                                    ds = SearchUtils.datasetFactory(conn, rs, modelProvider, metadataNames);
                                }
                                return true;
                            } catch(NoSuchElementException ex) {
                                return false;
                            } catch(SQLException ex) {
                                throw new RuntimeException("Error processing search results", ex);
                            }
                        }

                        @Override
                        public DatasetModel next(){
                            if(!hasNext()){
                                throw new NoSuchElementException();
                            }
                            DatasetModel ret = ds;
                            ds = null;
                            return ret;
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
                } catch(SQLException ex) {
                    throw new IOException("Error closing statement", ex);
                }
            }
        };

        return stream;
    }

    public static DirectoryStream<DatasetContainer> getContainers(final Connection conn,
            final ModelProvider modelProvider,
            final Select sel, final List<String> metadataNames) throws SQLException{
        System.out.println(sel.formatted());
        final PreparedStatement stmt = sel.prepareAndBind(conn);
        final ResultSet rs = stmt.executeQuery();
        if(!rs.next()){
            rs.close();
        }
        DirectoryStream<DatasetContainer> stream = new DirectoryStream<DatasetContainer>() {
            Iterator<DatasetContainer> iter = null;

            @Override
            public Iterator<DatasetContainer> iterator(){
                if(iter == null){
                    iter = new Iterator<DatasetContainer>() {
                        private DatasetContainer container = null;

                        @Override
                        public boolean hasNext(){
                            try {
                                while(container == null){
                                    if(rs.isClosed()){
                                        return false;
                                    }
                                    container = SearchUtils.containerFactory(conn, rs, modelProvider, metadataNames);
                                }
                                return true;
                            } catch(NoSuchElementException ex) {
                                return false;
                            } catch(SQLException ex) {
                                throw new RuntimeException("Error processing search results", ex);
                            }
                        }

                        @Override
                        public DatasetContainer next(){
                            if(!hasNext()){
                                throw new NoSuchElementException();
                            }
                            DatasetContainer ret = container;
                            container = null;
                            return ret;
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
                } catch(SQLException ex) {
                    throw new IOException("Error closing statement", ex);
                }
            }
        };
        return stream;
    }

    public static Map<String, String> getDependencySpec(List<String> l){
        HashMap<String, String> m = new HashMap<>();
        for (String s: l) {
            String[] a = s.split("\\.");
            String p = a.length > 0 ? a[0]: s;
            if (p.contains("dependency") || p.contains("dependents")) {
                if (a.length > 1){
                    m.put("type", a[1]);
                } else{
                    m.put("type", "*");
                }
            }
        }
        return m;
    }

    public static String getDependencyPath(Connection conn, long dependency) throws SQLException {
        String sql = "SELECT dependencyName from DatasetDependency WHERE Dependency = ?";
        String dependencyPath = "";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, dependency);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()){
                dependencyPath = rs.getString("dependencyName");
            }
        }
        return dependencyPath;
    }

    public static String getDependencyGroupPath(Connection conn, long dependency) throws SQLException {
        String sql = "SELECT dependencyName from DatasetDependency WHERE DependencyGroup = ?";
        String dependencyPath = "";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, dependency);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()){
                dependencyPath = rs.getString("dependencyName");
            }
        }
        return dependencyPath;
    }

    public static Map<String, Object> getDependency(Connection conn, String dependencyName)
        throws SQLException {
        String sql = "SELECT Dependency, DependencyGroup FROM DatasetDependency WHERE DependencyName = ?";
        HashMap<String, Object> result = new HashMap<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, dependencyName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                // found
                long dependency = rs.getLong("Dependency");
                if (dependency != 0) {
                    result.put("dependency", dependency);
                    result.put("dependencyContainer", "dependency");
                } else {
                    result.put("dependencyGroup", rs.getLong("DependencyGroup"));
                    result.put("dependencyContainer", "dependencyGroup");
                }
            }
        }
        return result;
    }

    public static Map<String, Object> getDependents(Connection conn, String dependencyContainer, Long dependency,
                                                    String filter) throws SQLException {
        String[] dependentTypes = SearchUtils.getDependentTypes(conn, dependencyContainer, dependency);
        Map<String, Object> metadata = new HashMap<>();
        String type="";
        boolean found = false;
        for (String dt : dependentTypes){
            String dts = dt + "s";
            if (dts.equals(filter)){
                type = dt;
                found = true;
                break;
            } else if (dt.equals(filter)){
                type = dt;
                break;
            }
        }

        if (filter == null || filter.isEmpty() || filter.equals("groups")){
            filter = "*";
        } else if (found){
            String node;
            if (dependencyContainer.equals("dependency")) {
                node = dependency.toString() + ".d";
            } else if (dependencyContainer.equals("dependencyGroup")){
                node = dependency.toString() + ".g";
            } else {
                throw new SQLException("Unrecognized dependency type");
            }
            Map<String, HashSet<String>> p = DependentSearch.getDependents(conn, node, type);
            metadata.put(filter, DependentSearch.getJson(p));
            return metadata;
        }

        for (String dt: dependentTypes){
            if (filter.equals(dt) || filter.equals("*")) {
                metadata.putAll(SearchUtils.getDependentsByType(conn, dependencyContainer, "dependent",
                    dependency, dt));
                metadata.putAll(SearchUtils.getDependentsByType(conn, dependencyContainer, "dependentGroup",
                    dependency, dt));
            }
        }
        return metadata;
    }

    public static void deleteDependentsByType(Connection conn, String dependencyContainer,
                                              Long dependency, String type) throws SQLException {
        if (type.equals("*")){
            String deleteSql = "DELETE FROM DatasetDependency WHERE " + dependencyContainer + " = ?";
            try (PreparedStatement stmt = conn.prepareStatement(deleteSql)){
                stmt.setLong(1, dependency);
                stmt.executeUpdate();
            }
        } else{
            String deleteSql = "DELETE FROM DatasetDependency WHERE " + dependencyContainer +
                " = ? and DependentType = ?";
            try (PreparedStatement stmt = conn.prepareStatement(deleteSql)){
                stmt.setLong(1, dependency);
                stmt.setString(2, type);
                stmt.executeUpdate();
            } catch (SQLException ex) {
                conn.rollback();
                if (ex.getMessage().toLowerCase().contains("deadlock")){
                    try {
                        Thread.sleep((int)(Math.random()*1000));
                    } catch (InterruptedException e){
                        throw new SQLException(e);
                    }
                    System.out.println("deleteDependentsByType: Deadlock detected...retrying");
                    deleteDependentsByType(conn, dependencyContainer, dependency, type);
                } else {
                    throw new SQLException(ex);
                }
            }
        }
    }


    public static Map<String, Object> getDependentsByType(Connection conn, String dependencyContainer,
                                                          String dependent,  Long dependency,
                                                          String type) throws SQLException {

        if (type.isEmpty() || type.equals("*")) {
            return SearchUtils.getDependents(conn, dependencyContainer, dependency, "*");
        }
        String sql = "SELECT dependencyName, " + dependent + " FROM DatasetDependency WHERE " +
            dependencyContainer + " = ? AND (dependentType = ? AND " + dependent + " IS NOT NULL)";
        HashMap<String, Object> metadata = new HashMap();
        try (PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, dependency);
            stmt.setString(2, type);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Long> dependents = new ArrayList<>();
            while (rs.next()) {
                dependents.add(rs.getLong(dependent));
            }
            // locate more dependents by the reciprocal nature of the relation
            if (Arrays.asList("predecessor", "successor").contains(type)) {
                Long[] moreDependents = SearchUtils.getReciprocalDependents(conn, dependencyContainer, dependent,
                    dependency, type.equals("predecessor") ? "successor" : "predecessor");
                for (Long d: moreDependents) {
                    if (!dependents.contains(d)){
                        dependents.add(d);
                    }
                }
            }
            if (!dependents.isEmpty()) {
                String dependentTag = type + ".";
                if (dependent.equals("dependentGroup")){
                    dependentTag += "group";
                } else{
                    dependentTag += "dataset";
                }
                StringBuilder dependentsAsStr= new StringBuilder();
                for (Long d: dependents){
                    dependentsAsStr.append(d.toString()).append(",");
                }
                if (dependentsAsStr.length()>0) {
                    // remove last ,
                    dependentsAsStr.deleteCharAt(dependentsAsStr.length()-1);
                }
                metadata.put(dependentTag, dependentsAsStr.toString());
            }
            return metadata;
        }
    }

    public static Long[] getReciprocalDependents(Connection conn, String dependencyContainer, String dependent,
                                                 Long dependentid, String type) throws SQLException {
        String sql = "SELECT " + dependencyContainer + ", dependencyName FROM DatasetDependency WHERE " +
            dependent + " = ? AND dependentType = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, dependentid);
            stmt.setString(2, type);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Long> dependents = new ArrayList<>();
            while (rs.next()) {
                long d = rs.getLong(dependencyContainer);
                if (d != 0) {
                    dependents.add(d);
                }
            }
            return dependents.toArray(new Long[0]);
        }
    }

    public static String[] getDependentTypes(Connection conn, String dependencyContainer,
                                             Long dependency) throws SQLException {
        String sql = "SELECT dependentType FROM DatasetDependency WHERE " + dependencyContainer + " = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, dependency);
            ResultSet rs = stmt.executeQuery();
            List<String> dependentTypes = new ArrayList<>();
            while (rs.next()) {
                String dt = rs.getString("dependentType");
                if (!dependentTypes.contains(dt)) {
                    dependentTypes.add(dt);
                }
            }
            // Check other types by reciprocal relation
            String[] addDependentTypes;
            if (dependencyContainer.equals("dependency")){
                addDependentTypes = SearchUtils.getDependentTypeReciprocal(conn, "dependent", dependency);
                for (String dt: addDependentTypes) {
                    if (!dependentTypes.contains(dt)){
                        dependentTypes.add(dt);
                    }
                }
            } else if (dependencyContainer.equals("dependencyGroup")) {
                addDependentTypes = SearchUtils.getDependentTypeReciprocal(conn, "dependentGroup", dependency);
                for (String dt : addDependentTypes) {
                    if (!dependentTypes.contains(dt)) {
                        dependentTypes.add(dt);
                    }
                }
            }
            return dependentTypes.toArray(new String[dependentTypes.size()]);
        }
    }

    public static String [] getDependentTypeReciprocal(Connection conn, String dependent, Long dependentid)
        throws SQLException {
        String sql = "SELECT dependentType FROM DatasetDependency WHERE " + dependent + " = ? ";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, dependentid);
            ResultSet rs = stmt.executeQuery();
            List<String> types = new ArrayList<String>();
            while (rs.next()) {
                String dt = rs.getString("dependentType");
                String dtR = dt.equals("predecessor") ? "successor": "predecessor";
                if (!types.contains(dtR)){
                    types.add(dtR);
                }
            }
            return types.toArray(new String[types.size()]);
        }
    }

    public static Class<?> getParamType(Object tRight){
        if(tRight instanceof List){
            List r = ((List) tRight);
            tRight = Collections.checkedList(r, r.get(0).getClass()).get(0);
        }
        if(tRight instanceof Number){
            return Number.class;
        }
        if(tRight instanceof Class){
            return (Class<?>) tRight;
        }
        return tRight.getClass();
    }
}

