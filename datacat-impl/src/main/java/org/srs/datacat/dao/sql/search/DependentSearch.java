package org.srs.datacat.dao.sql.search;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author Kenny Lo
 */
public final class DependentSearch {

    public static final String SQL1 = "select Dependency, Dependent, DependentType from DatasetDependency where " +
        "Dependency is not NULL and Dependent is not NULL";
    public static final String SQL2 = "select Dependency, DependentGroup, DependentType from DatasetDependency where " +
        "Dependency is not NULL and DependentGroup is not NULL";
    public static final String SQL3 = "select DependencyGroup, DependentGroup, DependentType from DatasetDependency " +
        "where DependencyGroup is not NULL and DependentGroup is not NULL";
    public static final String SQL4 = "select DependencyGroup, Dependent, DependentType from DatasetDependency where " +
        "DependencyGroup is not NULL and Dependent is not NULL";

    private DependentSearch() {
    }

    public static void add(HashMap<String, HashSet<String>> map, String key, String value){
        if (map.containsKey(key)) {
            map.get(key).add(value);
        } else{
            HashSet<String> a = new HashSet<>();
            a.add(value);
            map.put(key, a);
        }
    }

    public static  HashMap<String, HashSet<String>> getAllDependentsOfType(Connection conn, String dependentType)
        throws SQLException {
        HashMap<String, HashSet<String>> allDependents = new HashMap<>();
        String reciprocal;
        if (dependentType.equals("predecessor")){
            reciprocal = "successor";
        } else if (dependentType.equals("successor")){
            reciprocal = "predecessor";
        } else {
            reciprocal = dependentType;
        }
        // Case 1
        try (PreparedStatement stmt = conn.prepareStatement(SQL1)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Long dependency = rs.getLong("Dependency");
                Long dependent = rs.getLong("Dependent");
                String type = rs.getString("DependentType");
                if (type.equals(dependentType)) {
                    add(allDependents, dependency+".d", dependent+".d");
                } else if (type.equals(reciprocal)) {
                    add(allDependents, dependent+".d", dependency+".d");
                }
            }
        }
        // Case 2
        try (PreparedStatement stmt = conn.prepareStatement(SQL2)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Long dependency = rs.getLong("Dependency");
                Long dependent = rs.getLong("DependentGroup");
                String type = rs.getString("DependentType");
                if (type.equals(dependentType)) {
                    add(allDependents, dependency+".d", dependent+".g");
                } else if (type.equals(reciprocal)) {
                    add(allDependents, dependent+".g", dependency+".d");
                }
            }
        }
        // Case 3
        try (PreparedStatement stmt = conn.prepareStatement(SQL3)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Long dependency = rs.getLong("DependencyGroup");
                Long dependent = rs.getLong("DependentGroup");
                String type = rs.getString("DependentType");
                if (type.equals(dependentType)) {
                    add(allDependents, dependency+".g", dependent+".g");
                } else if (type.equals(reciprocal)) {
                    add(allDependents, dependent+".g", dependency+".g");
                }
            }
        }
        // Case 4
        try (PreparedStatement stmt = conn.prepareStatement(SQL4)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Long dependency = rs.getLong("DependencyGroup");
                Long dependent = rs.getLong("Dependent");
                String type = rs.getString("DependentType");
                if (type.equals(dependentType)) {
                    add(allDependents, dependency+".g", dependent+".d");
                } else if (type.equals(reciprocal)) {
                    add(allDependents, dependent+".d", dependency+".g");
                }
            }
        }
        return allDependents;
    }

    public static void doGetDependents(Map<String, HashSet<String>> retDependents, String node, String dependentType,
                                       HashMap<String, HashSet<String>> dependentTree){
        HashSet<String> nodeDependents = dependentTree.get(node);
        if (nodeDependents != null && !retDependents.containsKey(node)) {
            retDependents.put(node, nodeDependents);
            for (String dep : nodeDependents) {
                // check to see if dependent node already fetched
                if (!retDependents.containsKey(dep)) {
                    doGetDependents(retDependents, dep, dependentType, dependentTree);
                }
            }
        }
    }

    public static Map<String, HashSet<String>> getDependents(Connection conn, String node, String dependentType)
        throws SQLException{
        Map<String, HashSet<String>> retDependents = new HashMap<>();
        doGetDependents(retDependents, node, dependentType, getAllDependentsOfType(conn, dependentType));
        return retDependents;
    }

    public static String getJson(Map<String, HashSet<String>> map)
        throws SQLException{
        ObjectMapper mapper = new ObjectMapper();
        String json;
        try {
            json = mapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        return json;
    }
}