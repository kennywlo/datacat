package org.srs.datacat.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.srs.datacat.model.container.ContainerStat;

/**
 * Basic stat info for a group or a folder.
 *
 * @author bvan
 * @author klo
 */
@JsonTypeName(value="stat")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "_type", defaultImpl = BasicStat.class)
@JsonSubTypes(value = {@JsonSubTypes.Type(DatasetStat.class), @JsonSubTypes.Type(BasicStat.class)})
@JsonIgnoreProperties(ignoreUnknown=true)
public class BasicStat implements ContainerStat {

    /**
     * Type of Stat.
     */
    public static enum StatType {
        NONE,
        LAZY,
        BASIC,
        DATASET;
    }

    private int datasetCount;
    private int groupCount;
    private int folderCount;
    private int dependencyCount;

    public BasicStat(){}

    public BasicStat(int folderCount, int groupCount, int datasetCount, int dependencyCount){
        this.datasetCount = datasetCount;
        this.groupCount = groupCount;
        this.folderCount = folderCount;
        this.dependencyCount = dependencyCount;
    }

    public BasicStat(BasicStat stat){
        this(stat.folderCount, stat.groupCount, stat.datasetCount, stat.dependencyCount);
    }

    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getChildCount(){
        return folderCount + groupCount + datasetCount + dependencyCount;
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getDatasetCount(){
        return datasetCount;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getGroupCount(){
        return groupCount;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getFolderCount(){
        return folderCount;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getDependencyCount(){ return dependencyCount; }

    public void setDatasetCount(int datasets){
        this.datasetCount = datasets;
    }

    public void setGroupCount(int groups){
        this.groupCount = groups;
    }

    public void setFolderCount(int folders){
        this.folderCount = folders;
    }

    public void setDependencyCount(int dependencies){
        this.dependencyCount = dependencies;
    }

}
