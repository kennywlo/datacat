package org.srs.datacat.shared;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import junit.framework.TestCase;
import org.srs.datacat.shared.metadata.MetadataEntry;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

// ToDo: Dependency testing
public class DatasetDependencyTest extends TestCase {
    public DatasetDependencyTest() {
    }
    public void testDeserialization() throws IOException {

        String json_text = "{\"_type\":\"dependency\",\"versionMetadata\":[{\"dependencyName\":\"test\"},"
                + "{\"dependency\":12},"
                + "{\"dependentType\": \"predecessor\"},"
                + "{\"dependents\":\"43,78,87\"}]}";

        AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
        AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        AnnotationIntrospector pair = new AnnotationIntrospectorPair(primary, secondary);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setAnnotationIntrospector(pair);

        DatasetDependency dd = mapper.readValue(json_text, DatasetDependency.class);
        System.out.println(dd.toString());
    }
}
