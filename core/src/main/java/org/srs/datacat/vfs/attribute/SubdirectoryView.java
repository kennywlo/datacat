
package org.srs.datacat.vfs.attribute;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import org.srs.datacat.vfs.DcFileSystemProvider;
import org.srs.datacat.vfs.DcPath;
import org.srs.vfs.ChildrenView;

/**
 *
 * @author Brian Van Klaveren<bvan@slac.stanford.edu>
 */
public class SubdirectoryView extends ChildrenView<DcPath> {

    public SubdirectoryView(DcPath path){
        super( path );
    }

    @Override
    public String name(){
        return "subdirectories";
    }

    @Override
    protected void doRefreshCache() throws IOException{
        DcFileSystemProvider pro = getPath().getFileSystem().provider();
        try(DirectoryStream<DcPath> stream = 
                pro.unCachedDirectoryStream(getPath(), pro.AcceptAllFilter, false, false)){
            for(DcPath child: stream){
                children.put( child.getFileName().toString(), child );
            }
        }
    }

}
