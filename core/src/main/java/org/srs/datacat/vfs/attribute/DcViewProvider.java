
package org.srs.datacat.vfs.attribute;

import java.io.IOException;
import java.nio.file.attribute.FileAttributeView;
import org.srs.datacat.model.DatacatNode;

/**
 * Generic interface for view-providing classes that extend FileAttributeView.
 * @author bvan
 */
public interface DcViewProvider<T> extends FileAttributeView {
    
    DatacatNode withView(T viewDescriptor) throws IOException;

}
