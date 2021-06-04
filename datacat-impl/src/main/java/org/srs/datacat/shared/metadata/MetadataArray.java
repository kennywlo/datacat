package org.srs.datacat.shared.metadata;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.srs.datacat.shared.metadata.MetadataString.Builder;

/**
 *
 * @author klo
 */
@JsonTypeName(value = "[J")
@JsonDeserialize(builder = Builder.class)
public class MetadataArray implements MetadataValue<long[]> {
    private long[] value;

    public MetadataArray(){}

    public MetadataArray(long[] value){ this.value = value; }

    @Override
    @JsonValue
    public long[] getValue(){ return value; }

    @Override
    public String toString(){
        return String.format("string(\"%s\")", value);
    }

    /**
     * Builder.
     */
    public static class Builder extends MetadataValue.Builder<MetadataArray> {
        public Builder(){}

        public Builder(long[] val){
            super.value(val);
        }

        @Override
        public MetadataArray build(){
            return super.build();
        }

    }
}

