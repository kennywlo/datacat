
package org.srs.datacat.rest.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.srs.datacat.rest.BaseResource;
import org.srs.datacat.rest.SearchPluginProvider;
import org.srs.datacat.shared.DatacatObject;
import org.srs.datacat.shared.Dataset;
import org.srs.datacat.vfs.DcPath;
import org.srs.datacat.vfs.DcUriUtils;
import org.srs.datacat.vfs.DirectoryWalker.ContainerVisitor;
import org.srs.datacatalog.search.DatasetSearch;
import org.srs.rest.shared.RestException;
import org.srs.vfs.GlobToRegex;
import org.srs.vfs.PathUtils;
import org.zerorm.core.Select;

/**
 *
 * @author Brian Van Klaveren<bvan@slac.stanford.edu>
 */
@Path("/search")
public class SearchResource extends BaseResource {
    private final String searchRegex = "{id: [^\\?]+}";
    @Inject SearchPluginProvider pluginProvider;
    
    @GET
    @Path(searchRegex)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public Response find(@PathParam("id") String pathPattern,
            @QueryParam("recurse") boolean recurse,
            @QueryParam("sites") List<String> sites,
            @QueryParam("filter") String filter,
            @QueryParam("sort") List<String> sortParams, 
            @QueryParam("show") List<String> metadata, 
            /*@DefaultValue("false") @QueryParam("unscanned") boolean unscanned,*/
            /*@DefaultValue("false") @QueryParam("nonOk") boolean nonOk,*/
            @QueryParam("checkFolders") Boolean checkFolders,
            @QueryParam("checkGroups") Boolean checkGroups,
            /*@DefaultValue("false") @QueryParam("allMetadata") boolean metadata,*/
            @DefaultValue("-1") @QueryParam("max") int max,
            @DefaultValue("0") @QueryParam("offset") int offset) {

        pathPattern = "/" + pathPattern;
        List<? super Dataset> datasets = new ArrayList<>();
        String[] metafields= metadata.toArray( new String[0]);
        String[] sortFields = sortParams.toArray(new String[0]);
        
        try(Connection conn = getConnection()){
            DatasetSearch datacatSearch = new DatasetSearch(getProvider(), conn, pluginProvider.getPlugins());

            String queryString = filter;

            String searchBase = PathUtils.normalizeRegex(GlobToRegex.toRegex(pathPattern,"/"));
            DcPath root = getProvider().getPath(DcUriUtils.toFsUri("/", null, "SRS"));
            DcPath searchPath = root.resolve(searchBase);
            ContainerVisitor visitor = new ContainerVisitor(searchPath.getFileSystem(), pathPattern, checkGroups, checkFolders);
            Select stmt = datacatSearch.compileStatement( conn, searchPath, visitor, 
                            false, 100, queryString, null, metafields, sortFields,0,-1);
            datasets = datacatSearch.searchForDatasetsInParent(conn, stmt);
            System.out.println(datasets.size());
        } catch (FileNotFoundException ex){
             throw new RestException(ex,404, "File doesn't exist", ex.getMessage());
        } catch(SQLException | IOException ex) {
            throw new RestException(ex, 500);
        } catch(ParseException ex) {
            throw new RestException(ex, 422, "Unable to parse filter", ex.getMessage());
        }
        return Response
                .ok( new GenericEntity<List<DatacatObject>>((List<DatacatObject>) datasets) {})
                .build();
    }
    
}   
