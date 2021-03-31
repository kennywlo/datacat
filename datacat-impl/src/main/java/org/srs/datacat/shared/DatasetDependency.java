
package org.srs.datacat.shared;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.srs.datacat.model.DatacatNode;
import org.srs.datacat.model.DatasetContainer.Dependency;
import org.srs.datacat.shared.DatasetDependency.Builder;

/**
 * A DatasetDependency is  a special container which contains only dependents, e.g. predecessors.
 *
 * @author klo
 */
@JsonTypeName(value="dependency")
@JsonDeserialize(builder=Builder.class)
public class DatasetDependency extends DatasetContainer implements Dependency {

    public DatasetDependency(){
        super();
    }

    /**
     * Copy constructor.
     *
     * @param dependency
     */
    public DatasetDependency(DatasetDependency dependency){
        super(dependency);
    }

    public DatasetDependency(DatacatObject.Builder builder){
        super(builder);
    }

    public DatasetDependency(DatasetContainerBuilder builder){
        super(builder);
    }

    /**
     * Builder.
     */
    public static class Builder extends DatasetContainerBuilder {

        public Builder(){ super(); }
        public Builder(DatacatObject object){ super(object); }
        public Builder(DatacatNode object){ super(object); }
        public Builder(DatasetContainerBuilder builder){ super(builder); }

        @Override
        public DatasetDependency build(){
            return new DatasetDependency(this);
        }
    }
}
