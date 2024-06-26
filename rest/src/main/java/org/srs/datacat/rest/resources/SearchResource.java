package org.srs.datacat.rest.resources;

import org.srs.datacat.dao.sql.search.SearchUtils;
import org.srs.datacat.model.*;
import org.srs.datacat.rest.BaseResource;
import org.srs.datacat.rest.RestException;
import org.srs.datacat.rest.SearchPluginProvider;
import org.srs.datacat.shared.RequestView;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.srs.datacat.rest.BaseResource.OPTIONAL_EXTENSIONS;

/**
 *
 * @author bvan
 */
@Path("/search" + OPTIONAL_EXTENSIONS)
public class SearchResource extends BaseResource {
    private final String searchRegex = "{id: [^\\?]+}";
    @Inject SearchPluginProvider pluginProvider;

    private UriInfo ui;
    private List<PathSegment> pathSegments;
    private String requestPath;
    private HashMap<String, List<String>> requestMatrixParams = new HashMap<>();
    private HashMap<String, List<String>> requestQueryParams = new HashMap<>();

    public SearchResource(@PathParam("id") List<PathSegment> pathSegments, @Context UriInfo ui){
        this.pathSegments = pathSegments;
        this.ui = ui;
        String path = "";
        if(pathSegments != null){
            for(PathSegment s: pathSegments){
                path = path + "/" + s.getPath();
                requestMatrixParams.putAll(s.getMatrixParameters());
            }
        }
        requestPath = path;
        requestQueryParams.putAll(ui.getQueryParameters());
    }

    @GET
    @Path(searchRegex)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public Response find(
            @QueryParam("recurse") boolean recurse,
            @QueryParam("sites") List<String> sites,
            @QueryParam("filter") String filter,
            @QueryParam("containerFilter") String containerFilter,
            @QueryParam("sort") List<String> sortParams,
            @QueryParam("show") List<String> metadata,
            @QueryParam("ignoreShowKeyError") boolean ignoreShowKeyError,
            @DefaultValue("100000") @QueryParam("max") int max,
            @DefaultValue("0") @QueryParam("offset") int offset){

        String pathPattern = requestPath;
        String[] metafields = metadata.toArray(new String[0]);
        String[] sortFields = sortParams.toArray(new String[0]);

        DatasetView dv = getDatasetView();

        try {
            // fetch the dataset groups in dependents search
            if (containerFilter != null && containerFilter.contains("dependentGroups")){
                DirectoryStream<DatasetContainer> stream = getProvider().searchContainers(Arrays.asList(requestPath),
                    buildCallContext(), containerFilter, metafields, sortFields, ignoreShowKeyError);
                List<DatasetContainer> groups = new ArrayList<>();
                int count = 0;
                Iterator<DatasetContainer> iter = stream.iterator();
                for(int i = 0; iter.hasNext(); i++, count++){
                    if(i >= offset && i < (offset + max)){
                        groups.add(iter.next());
                    } else {
                        iter.next();
                    }
                }
                stream.close();
                ContainerResultSetModel searchResults = getProvider().getModelProvider().getContainerResultSetBuilder()
                    .results(groups).count(count).build();
                return Response.ok(new GenericEntity<ContainerResultSetModel>(searchResults) {}).build();
            }

            Map<String, String> m;
            boolean skip = false;
            if (filter != null && filter.contains("dependents")) {
                // case of dependent search by pk, just proceed
                ;
            } else if (!(m=SearchUtils.getDependencySpec(metadata)).isEmpty()){
                // transform the query request in dependents search
                try {
                    Connection conn = getConnection();
                    Map<String, Object> r = SearchUtils.getDependency(conn, requestPath);
                    if (r.isEmpty()){
                        // no dependents; just return empty set
                        skip = true;
                    } else{
                        String depContainer = (String) r.get("dependencyContainer");
                        Long pk = (Long) r.get("dependency");
                        Map<String, Object> dependents = SearchUtils.getDependents(conn, depContainer,
                            pk, m.get("type"));
                        String dep_ids = "";
                        for (String key : dependents.keySet()) {
                            if (!key.contains("dataset")) {
                                // should only return datasets
                                continue;
                            }
                            if (dep_ids.isEmpty()) {
                                dep_ids = (String) dependents.get(key);
                            } else {
                                dep_ids += ("," + dependents.get("key"));
                            }
                        }
                        if (!dep_ids.isEmpty()) {
                            filter = "dependents in (" + dep_ids + ")";
                        } else{
                            // no dependents; just return empty set
                            skip = true;
                        }
                    }
                    conn.close();
                } catch (SQLException ex) {
                    throw new IOException(ex);
                }
            }

            // fetch the datasets
            int count = 0;
            List<DatasetModel> datasets = new ArrayList<>();
            if (!skip) {
                DirectoryStream<DatasetModel> stream = getProvider().search(Arrays.asList(pathPattern), buildCallContext(),
                    dv, filter, containerFilter, metafields, sortFields, ignoreShowKeyError);

                Iterator<DatasetModel> iter = stream.iterator();
                for (int i = 0; iter.hasNext(); i++, count++) {
                    if (i >= offset && i < (offset + max)) {
                        datasets.add(iter.next());
                    } else {
                        iter.next();
                    }
                }
                stream.close();
            }
            DatasetResultSetModel searchResults = getProvider().getModelProvider().getDatasetResultSetBuilder()
                    .results(datasets).count(count).build();
            return Response.ok(new GenericEntity<DatasetResultSetModel>(searchResults) {}).build();
        } catch(IllegalArgumentException ex) {
            throw new RestException(ex, 400, "Unable to process query, see message", ex.getMessage());
        } catch(NoSuchFileException ex) {
            throw new RestException(ex, 404, "File doesn't exist", ex.getMessage());
        } catch(IOException | RuntimeException ex) {
            Logger.getLogger(SearchResource.class.getName()).log(Level.WARNING, "Unknown exception", ex);
            ex.printStackTrace();
            throw new RestException(ex, 500);
        } catch(ParseException ex) {
            throw new RestException(ex, 422, "Unable to parse filter", ex.getMessage());
        }

    }

    @POST
    @Path(searchRegex)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public Response find(
            @FormParam("targets") List<String> targets,
            @FormParam("recurse") boolean recurse,
            @FormParam("sites") List<String> sites,
            @FormParam("filter") String filter,
            @FormParam("containerFilter") String containerFilter,
            @FormParam("sort") List<String> sortParams,
            @FormParam("show") List<String> metadata,
            @FormParam("ignoreShowKeyError") boolean ignoreShowKeyError,
            @DefaultValue("100000") @FormParam("max") int max,
            @DefaultValue("0") @FormParam("offset") int offset){

        String[] metafields = metadata.toArray(new String[0]);
        String[] sortFields = sortParams.toArray(new String[0]);

        DatasetView dv = getDatasetView();

        DatasetResultSetModel searchResults = null;
        try(DirectoryStream<DatasetModel> stream
                = getProvider().search(targets, buildCallContext(), dv, filter,
                        containerFilter, metafields, sortFields, ignoreShowKeyError)) {
            List<DatasetModel> datasets = new ArrayList<>();
            int count = 0;
            Iterator<DatasetModel> iter = stream.iterator();
            for(int i = 0; iter.hasNext(); i++, count++){
                if(i >= offset && i < (offset + max)){
                    datasets.add(iter.next());
                } else {
                    iter.next();
                }
            }
            searchResults = getProvider().getModelProvider().getDatasetResultSetBuilder()
                    .results(datasets).count(count).build();
        } catch(IllegalArgumentException ex) {
            throw new RestException(ex, 400, "Unable to process query, see message", ex.getMessage());
        } catch(NoSuchFileException ex) {
            throw new RestException(ex, 404, "File doesn't exist", ex.getMessage());
        } catch(IOException | RuntimeException ex) {
            Logger.getLogger(SearchResource.class.getName()).log(Level.WARNING, "Unknown exception", ex);
            ex.printStackTrace();
            throw new RestException(ex, 500);
        } catch(ParseException ex) {
            throw new RestException(ex, 422, "Unable to parse filter", ex.getMessage());
        }
        return Response.ok(new GenericEntity<DatasetResultSetModel>(searchResults) {}).build();
    }

    private DatasetView getDatasetView(){
        try {
            RequestView rv = new RequestView(RecordType.DATASET, requestMatrixParams);
            if(rv.getPrimaryView() == RequestView.CHILDREN || rv.getPrimaryView() == RequestView.METADATA){
                throw new IllegalArgumentException("Children and Metadata views not available when searching");
            }
            return rv.getDatasetView(DatasetView.MASTER);
        } catch(IllegalArgumentException ex) {
            throw new RestException(ex, 400, "Unable to process view", ex.getMessage());
        }
    }
}
