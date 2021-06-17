package org.srs.datacat.shared;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JacksonStdImpl;

import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import junit.framework.TestCase;
import org.srs.datacat.shared.metadata.MetadataEntry;

import java.io.IOException;
import java.io.StringWriter;



public class DatasetDependencyTest extends TestCase {
    public DatasetDependencyTest() {
    }
    public void testDeserialization() throws IOException {
        /**
         * @author CesarAxel
         */
        /**
         * In this test we are ensuring that we can properly generate deserialize a JSON object to a Java Object
         */

        String json_text2 = "{\"_type\":\"dependency\",\"metadata\":[{\"name\":\"test\"},"
                + "{\"dependency\":12345678},"
                + "{\"dependent\":87654321},"
                + "{\"dependentType\":\"predecessor\"}]}";

        AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
        AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        AnnotationIntrospector pair = new AnnotationIntrospectorPair(primary, secondary);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setAnnotationIntrospector(pair);
        DatasetDependency dd = mapper.readValue(json_text2, DatasetDependency.class);

        assertEquals("test", dd.getMetadata().get(0).getRawValue());
        assertEquals("12345678", dd.getMetadata().get(1).getRawValue().toString());
        assertEquals("87654321", dd.getMetadata().get(2).getRawValue().toString());
        assertEquals("predecessor", dd.getMetadata().get(3).getRawValue());

    }
}
