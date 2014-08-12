
package org.srs.datacat.shared;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import org.srs.datacat.shared.dataset.DatasetBuilder;
import org.srs.datacat.shared.dataset.FlatDataset;
import org.srs.datacat.shared.dataset.FullDataset;
import org.srs.datacat.shared.dataset.VersionWithLocations;
import org.srs.rest.shared.metadata.MetadataEntry;

/**
 * The generalized base object for most of the database derived objects.
 * @author bvan
 */
@XmlRootElement
@XmlSeeAlso({LogicalFolder.class, DatasetGroup.class, 
    Dataset.class, FlatDataset.class, FullDataset.class,
    DatasetVersion.class, VersionWithLocations.class, DatasetLocation.class})
@XmlType(propOrder={"name","path","pk","parentPk","metadata"})
@JsonPropertyOrder({"name","path","pk","parentPk","metadata"})
// Use default implementation of DatacatObject
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="$type", defaultImpl=DatacatObject.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(LogicalFolder.class), @JsonSubTypes.Type(DatasetGroup.class),
    @JsonSubTypes.Type(Dataset.class), 
    @JsonSubTypes.Type(FlatDataset.class),
    @JsonSubTypes.Type(FullDataset.class),
    @JsonSubTypes.Type(DatasetVersion.class),
    @JsonSubTypes.Type(VersionWithLocations.class),
    @JsonSubTypes.Type(DatasetLocation.class)
})
@JsonDeserialize(builder=DatacatObjectBuilder.class)
public class DatacatObject implements Serializable {

    private Long pk;
    private Long parentPk;
    private String name;
    private LinkedList<DatacatObject> pathList;
    private HashMap<String,String> stringMetadata = null;
    private HashMap<String,Number> numberMetadata = null;
    
    @XmlEnum(String.class)
    public enum Type {
        DATASET,
        DATASETLOCATION,
        DATASETVERSION,
        FOLDER,
        GROUP;
        
        public boolean isContainer(){
            return this == FOLDER || this == GROUP;
        }
        
        public static Type typeOf(DatacatObject object){
            if(object instanceof LogicalFolder)
                return FOLDER;
            if(object instanceof Dataset)
                return DATASET;
            if(object instanceof DatasetGroup)
                return GROUP;
            return null;
        }        
    }
    
    // Default no-arg constructor needed for jaxb
    public DatacatObject(){}
    
    public DatacatObject(Long pk, Long parentPk, String name){
        this.pk = pk;
        this.parentPk = parentPk;
        this.name = name;
    }
    
    /**
     * Cloning constructor
     * @param object 
     */
    public DatacatObject(DatacatObject object){
        this(object.pk, object.parentPk, object.name);
        initPath( object.getPath() );
        numberMetadata = object.numberMetadata!=null ? 
                new HashMap<>(object.numberMetadata) : null;
        stringMetadata = object.stringMetadata!=null ? 
                new HashMap<>(object.stringMetadata) : null;
    }
    
    public DatacatObject(DatacatObjectBuilder builder){
        this(builder.pk, builder.parentPk, builder.name);
        initPath( builder.path );
        numberMetadata = builder.numberMetadata!=null ? 
                new HashMap<String,Number>(builder.numberMetadata) : null;
        stringMetadata = builder.stringMetadata!=null ? 
                new HashMap<String,String>(builder.stringMetadata) : null;
        if(builder.metadata != null){
            initMetadata(builder.metadata);
        }
    }
    
    @XmlElement(required=false)
    public String getName(){
        return name;
    }
    
    /**
     * Get the primary key for the database object in question.
     * This will correspond to the object's primary key column.
     * @return The primary key for the given object in the database.
     */
    @XmlElement(required=false)
    public Long getPk(){
        return pk;
    }

    /**
     * Get the primary key for the database objects parent object.
     * Every object has a parent object.
     * Child : Parent
     * Folder : Folder
     * Group : Folder
     * Dataset : Group || Folder
     * DatasetVersion : Dataset
     * DatasetLocation : DatasetVersion
     * 
     * @return The primary key for the given object's parent
     */
    @XmlElement(required=false)
    public Long getParentPk(){
        // TODO: Should we return -1 if the parent isn't set?
        return parentPk;
    }

    /**
     * If the object has it's path information included, we will return it.
     * If not, we will return null.
     * @return The path of the object in the datacatalog as a string.
     * @return null
     */
    @XmlElement(required=false)
    public String getPath(){
        if(pathList == null)
            return null;
        String s = "";
        for(DatacatObject o: pathList){
            s = s + '/' + o.getName();
        }
        return s;
    }

    /**
     * If the object has had a parent set, we will return it.
     * @return Parent object or null
     */
    @XmlTransient
    public DatacatObject getParentDatacatObject(){
        return pathList != null && parentPk !=0 ? pathList.get(pathList.size()-2) : null;
    }
    
    public static <S extends DatacatObject> S asType(Class<S> type){
        DatacatObject o = null;
        try {
            o = type.newInstance();
        } catch(Exception ex) { }
        return (S) o;
    }

    /**
     * Internally, the path is stored as a linked list of basic objects.
     * @return The path as a linked list
     */
    @XmlTransient
    public LinkedList<DatacatObject> getPathList(){
        return pathList;
    }

    /**
     * We store the metadata internally as two seperate maps, one for strings
     * and one for numbers.
     * @return The meatadata with values of String types
     */
    @XmlTransient
    public HashMap<String,String> getStringMetadata() {
        return stringMetadata;
    }

     /**
     * We store the metadata internally as two seperate maps, one for strings
     * and one for numbers.
     * @return The meatadata with values of Number types
     */
    @XmlTransient
    public HashMap<String,Number> getNumberMetadata() {
        return numberMetadata;
    }
    
    /**
     * Convenience method to lump metadata into a list for easy serialization.
     * This makes the xml and json output a lot cleaner.
     * @return 
     */
    @XmlElementWrapper(name="metadata")
    @XmlElement(required=false, name="entry")
    @JsonProperty("metadata")
    public List<MetadataEntry> getMetadata() {
        ArrayList<MetadataEntry> entries = new ArrayList<>();
        if(numberMetadata != null){
            for(Entry<String, Number> e: numberMetadata.entrySet()){
                entries.add( new MetadataEntry( e.getKey(), e.getValue() ) );
            }
        }
        if(stringMetadata != null){
            for(Entry<String,String> e: stringMetadata.entrySet()){
                entries.add( new MetadataEntry(e.getKey(), e.getValue()));
            }
        }
        return entries.size() > 0 ? entries : null;
    }
    
    @XmlTransient
    public String getType(){
        XmlType t = getClass().getAnnotation( XmlType.class );
        return t.name();
    }
    
    public boolean isType(Type type){
        return Type.typeOf(this) == type;
    }
    
    private void initMetadata(List<MetadataEntry> metadataMap) {
        numberMetadata = new HashMap<>();
        stringMetadata = new HashMap<>();
        
        for(MetadataEntry e: metadataMap){
            if(e.getRawValue() instanceof Number) {
                numberMetadata.put(e.getKey(), (Number)e.getRawValue());
            } else {
                stringMetadata.put(e.getKey(), (String)e.getRawValue());
            }
        }
        // If we didn't set any values, set the map back to null
        stringMetadata = stringMetadata.isEmpty() ? null : stringMetadata;
        numberMetadata = numberMetadata.isEmpty() ? null : numberMetadata;
    }
    
    private void initPath(String path){
        if(path == null){
            return;
        }
        this.pathList = new LinkedList<>();
        String[] sgmts = path.split("/");
        DatacatObjectBuilder o;
        // First split will be empty string because path should start with a /
        for(int i = 1; i < sgmts.length; i++){
            if (i == sgmts.length - 2 && this instanceof Dataset) {
                o = new DatasetGroup.Builder();
            } else {
                o = new LogicalFolder.Builder();
            }
            o.name(sgmts[i]);
            this.pathList.add(o.build());
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( omitNull("Name: ", name));
        sb.append( omitNull("Path: ", getPath()));
        List me = getMetadata();
        if(me != null && !me.isEmpty()){
            sb.append("Metadata: {");
            for(Iterator iter = me.iterator(); iter.hasNext();){
                sb.append(iter.next()).append( iter.hasNext()? ", " :"");
            }
            sb.append( "}");
        }
        return sb.toString();
    }

    protected String omitNull(String name, Object o){
        return o != null ? name + o + "\t": "";
    }
    
    //@XmlTransient
    public static DatacatObjectBuilder builder(String type){
        switch (Type.valueOf(type)){
            case DATASET:
                return DatasetBuilder.create().type( type );
            case FOLDER:
                return new LogicalFolder.Builder();
            case GROUP:
                return new DatasetGroup.Builder();
            default:
                return new DatacatObjectBuilder<>().type( type );
        }
    }

    /**
     * Checks to make sure any fields that are declared are equivalent
     * @param obj
     * @return 
     */
    public boolean weakEquals(Object obj){
        if(obj == null){
            return false;
        }
        if(getClass() != obj.getClass()){
            return false;
        }
        final DatacatObject other = (DatacatObject) obj;
        if(!weakEquivalence(this.pk, other.pk)){
            return false;
        }
        if(!weakEquivalence(this.parentPk, other.parentPk)){
            return false;
        }
        if(!weakEquivalence(this.name, other.name)){
            return false;
        }
        if(!weakEquivalence(this.pathList, other.pathList)){
            return false;
        }
        return true;
    }
    
    public static boolean weakEquivalence(Object source, Object target){
        if(target == null || Objects.equals(source, target)){
            return true;
        }
        return false;
    }
    
}
