
package org.srs.datacat.model;

import java.util.List;

/**
 * A result set from a search operation.
 * @author kennylo
 */
public interface ContainerResultSetModel extends Iterable<DatasetContainer> {

    /**
     * Return the datasets that matched the search.
     * @return List of DatasetContainers
     */
    List<DatasetContainer> getResults();

    /**
     * The total count of all results.
     * @return the count
     */
    Integer getCount();

    /**
     * Version Builder interface.
     */
    public interface Builder {

        ContainerResultSetModel build();

        Builder results(List<DatasetContainer> val);
        Builder count(Integer val);

    }

}