
package org.srs.datacat.shared;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.srs.datacat.model.DatasetModel;
import java.sql.Timestamp;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.srs.datacat.model.DatacatNode;
import org.srs.datacat.model.dataset.DatasetLocationModel;
import org.srs.datacat.model.dataset.DatasetVersionModel;
import org.srs.datacat.model.DatasetView;
import org.srs.datacat.model.dataset.DatasetViewInfoModel;
import org.srs.datacat.shared.Dataset.Builder;
import org.srs.datacat.shared.metadata.MetadataEntry;

/**
 * Represents an entire dataset. Subclasses may include information on all DatasetVersions.
 * @author bvan
 */
@JsonTypeName(value="dataset")
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="_type", defaultImpl=Dataset.class)
@JsonDeserialize(builder=Builder.class)
@JsonPropertyOrder({"_type", "name",  "path", "pk", "parentPk",
    "metadata", "dataType", "fileFormat", "created"})
public class Dataset extends DatacatObject implements DatasetModel {
    
    private String fileFormat;
    private String dataType;
    private Timestamp dateCreated;
    
    public Dataset(){ super(); }
    
    public Dataset(DatacatObject o){ super(o); }
    
    /**
     * Copy constructor.
     *
     * @param dataset 
     */
    public Dataset(Dataset dataset){
        super(dataset);
        this.dataType = dataset.dataType;
        this.fileFormat = dataset.fileFormat;
        this.dateCreated = dataset.dateCreated;
    }
    
    public Dataset(DatacatObject.Builder builder){
        super(builder);
    }
    
    public Dataset(Builder builder){
        super(builder);
        this.dataType = builder.dataType;
        this.fileFormat = builder.fileFormat;
        this.dateCreated = builder.created;
    }
    
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDataType() { return this.dataType; }
        
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getFileFormat() { return this.fileFormat; }
    
    @Override
    @JsonProperty("created")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Timestamp getDateCreated(){ return this.dateCreated; }
    
    @Override
    @JsonIgnore
    public Timestamp getDateModified(){
        return null;
    }
    
    @Override
    public String toString() {
        return super.toString() + "\tType: " + dataType + "\tCreated: " + dateCreated;
    }
   
    @JsonIgnore
    public List<DatasetView> getDatasetViews(){
        return Collections.singletonList(DatasetView.EMPTY);
    }

    /**
     * Dataset Builder.
     * 
     * @author bvan
     */
    public static class Builder extends DatacatObject.Builder<org.srs.datacat.model.DatasetModel.Builder> 
        implements org.srs.datacat.model.DatasetModel.Builder {

        //public static final int MULTI = BASE | VERSIONS | LOCATIONS;
        public int dsType = BASE;
        public DatasetVersionModel version;
        public DatasetLocationModel location;
        public HashMap<String, Object> versionMetadata = new HashMap<>();
        public List<DatasetVersion> versions;
        public Collection<DatasetLocationModel> locations;
        // mixins
        public Timestamp created;
        public Long versionPk;
        public Long locationPk;
        public Boolean latest;
        public Boolean master;
        public String fileFormat;
        public String dataType;
        public Integer versionId;
        public String datasetSource;
        public Long processInstance;
        public String taskName;
        public Timestamp versionCreated;
        public Timestamp versionModified;
        public Long size;
        public String resource;
        public String site;
        public Long eventCount;
        public Long runMin;
        public Long runMax;
        public String checksum;
        public Timestamp locationCreated;
        public Timestamp locationModified;
        public Timestamp locationScanned;
        public String scanStatus;

        public Builder(){
            super();
        }

        /**
         * Copy constructor.
         * 
         * @param builder
         */
        public Builder(Builder builder){
            super( builder );
            this.dsType = builder.dsType;
            this.version = builder.version;
            this.location = builder.location;
            this.versions = builder.versions;
            this.locations = builder.locations;
            this.created = builder.created;
            this.versionPk = builder.versionPk;
            this.locationPk = builder.locationPk;
            this.latest = builder.latest;
            this.master = builder.master;
            this.fileFormat = builder.fileFormat;
            this.dataType = builder.dataType;
            this.versionId = builder.versionId;
            this.datasetSource = builder.datasetSource;
            this.processInstance = builder.processInstance;
            this.taskName = builder.taskName;
            this.versionCreated = builder.versionCreated;
            this.versionModified = builder.versionModified;
            this.versionMetadata = builder.versionMetadata;
            this.size = builder.size;
            this.resource = builder.resource;
            this.site = builder.site;
            this.eventCount = builder.eventCount;
            this.runMin = builder.runMin;
            this.runMax = builder.runMax;
            this.checksum = builder.checksum;
            this.locationCreated = builder.locationCreated;
            this.locationModified = builder.locationModified;
            this.locationScanned = builder.locationScanned;
            this.scanStatus = builder.scanStatus;
        }
        
        public Builder(DatasetModel ds){
            super(ds);
            this.fileFormat = ds.getFileFormat();
            this.dataType = ds.getDataType();
            this.created = ds.getDateCreated();
        }
        
        public Builder(DatacatNode ds){
            super(ds);
        }

        @Override
        public Builder create(DatacatNode val){
            if(val instanceof DatasetModel){
                return new Builder((DatasetModel) val);
            }
            return new Builder(val);
        }
        
        @Override
        public Builder view(DatasetViewInfoModel view){
            if(view.versionOpt().isPresent()){
                version(view.getVersion());
            }
            if(view.singularLocationOpt().isPresent()){
                location(view.singularLocationOpt().get());
            } else if (view.locationsOpt().isPresent()){
                locations(view.getLocations());
            }
            return this;
        }

        @JsonSetter
        @Override
        public Builder version(DatasetVersionModel val){
            this.version = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder location(DatasetLocationModel val){
            this.location = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder created(Timestamp val){
            this.created = checkTime(val);
            dsType |= BASE;
            return this;
        }

        @JsonSetter
        @Override
        public Builder fileFormat(String val){
            this.fileFormat = val;
            dsType |= BASE;
            return this;
        }

        @JsonSetter
        @Override
        public Builder dataType(String val){
            this.dataType = val;
            dsType |= BASE;
            return this;
        }

        @JsonSetter
        @Override
        public Builder versionPk(Long val){
            this.versionPk = val;
            dsType |= BASE;
            return this;
        }

        @JsonSetter
        @Override
        public Builder locationPk(Long val){
            this.locationPk = val;
            dsType |= VERSION;
            return this;
        }

        @JsonIgnore
        @Override
        public Builder versionId(Integer val){
            this.versionId = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder versionId(DatasetView.VersionId val){
            return versionId( val.getId() );
        }

        @JsonSetter
        @Override
        public Builder datasetSource(String val){
            this.datasetSource = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        public Builder processInstance(Long val){
            this.processInstance = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        public Builder taskName(String val){
            this.taskName = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder latest(Boolean val){
            this.latest = val;
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder versionCreated(Timestamp val){
            this.versionCreated = checkTime(val);
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder versionModified(Timestamp val){
            this.versionModified = checkTime(val);
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        public Builder locations(Collection<DatasetLocationModel> val){
            this.locations = val;
            dsType |= LOCATIONS;
            return this;
        }

        @JsonSetter
        public Builder versionMetadata(List<MetadataEntry> val){
            this.versionMetadata = new HashMap<>();
            if(val != null){
                for(MetadataEntry e: val){
                    if(e.getRawValue() instanceof Number) {
                        versionMetadata.put(e.getKey(), (Number)e.getRawValue());
                    } else if(e.getRawValue() instanceof Timestamp) {
                        versionMetadata.put(e.getKey(), (Timestamp) e.getRawValue());
                    } else {
                        versionMetadata.put(e.getKey(), (String)e.getRawValue());
                    }
                }   
            }
            dsType |= VERSION;
            return this;
        }

        @Override
        public Builder versionMetadata(Map<String, Object> val){
            this.versionMetadata = new HashMap<>();
            if(val != null){
                this.versionMetadata.putAll( val );
            }
            dsType |= VERSION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder size(Long val){
            this.size = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder resource(String val){
            this.resource = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        public Builder eventCount(Long val){
            this.eventCount = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder site(String val){
            this.site = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        public Builder runMin(Long val){
            this.runMin = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        public Builder runMax(Long val){
            this.runMax = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder checksum(String val){
            this.checksum = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder master(Boolean val){
            this.master = val;
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder locationCreated(Timestamp val){
            this.locationCreated = checkTime(val);
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder locationModified(Timestamp val){
            this.locationModified = checkTime(val);
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder locationScanned(Timestamp val){
            this.locationScanned = checkTime(val);
            dsType |= LOCATION;
            return this;
        }

        @JsonSetter
        @Override
        public Builder scanStatus(String val){
            this.scanStatus = val;
            dsType |= LOCATION;
            return this;
        }

        @Override
        public Dataset build(){
            switch(dsType){
                case FULL:
                case BASE | LOCATIONS:
                    return new FullDataset.Builder(this).build();
                case FLAT:
                case BASE | VERSION:
                case BASE | LOCATION:
                    return new FlatDataset.Builder(this).build();
                default:
                    return new Dataset( this );
            }
        }
        
        public boolean checkType(int type){
            return (dsType & type) != 0;
        }
    }
}
