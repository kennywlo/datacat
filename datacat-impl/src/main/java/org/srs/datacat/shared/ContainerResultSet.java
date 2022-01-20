package org.srs.datacat.shared;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Iterator;
import java.util.List;
import org.srs.datacat.model.DatasetContainer;
import org.srs.datacat.model.ContainerResultSetModel;

/**
 * Implementation of ContainerResultSetModel with Jackson annotations.
 * @author kennylo
 */
@JsonTypeName(value="containerSearchResults")
@JsonDeserialize(builder= ContainerResultSet.Builder.class)
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="_type", defaultImpl=ContainerResultSet.class)
public class ContainerResultSet implements ContainerResultSetModel {

    private List<DatasetContainer> results;
    private Integer count;

    public ContainerResultSet(List<DatasetContainer> results, Integer count){
        this.results = results;
        this.count = count;
    }

    @Override
    public List<DatasetContainer> getResults(){
        return results;
    }

    @Override
    public Integer getCount(){
        return count;
    }

    @Override
    @JsonIgnore
    public Iterator<DatasetContainer> iterator(){
        return results.iterator();
    }

    /**
     * Implementation of Builder.
     */
    public static class Builder implements ContainerResultSetModel.Builder {

        private List<DatasetContainer> results;
        private Integer count;

        public Builder(){ }

        @Override
        public ContainerResultSet build(){
            return new ContainerResultSet(results, count);
        }

        @Override
        @JsonSetter
        public Builder results(List<DatasetContainer> val){
            this.results = val;
            return this;
        }

        @Override
        @JsonSetter
        public Builder count(Integer val){
            this.count = val;
            return this;
        }

    }

}
