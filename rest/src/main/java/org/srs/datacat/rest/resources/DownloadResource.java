package org.srs.datacat.rest.resources;


import org.srs.datacat.rest.BaseResource;
import org.srs.datacat.rest.RestException;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import static org.srs.datacat.rest.BaseResource.OPTIONAL_EXTENSIONS;

/**
  * The DataCatalog resource will return the file for given dataset location.
  * @author kennylo
 **/
@Path("/DataCatalog" + OPTIONAL_EXTENSIONS)
public class DownloadResource extends BaseResource {
    private final String downloadRegex = "{verb: [^\\?]+}";

    private UriInfo ui;
    private List<PathSegment> pathSegments;
    private String requestPath;
    private HashMap<String, List<String>> requestMatrixParams = new HashMap<>();
    private HashMap<String, List<String>> requestQueryParams = new HashMap<>();

    public DownloadResource(@PathParam("verb") List<PathSegment> pathSegments, @Context UriInfo ui){
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
    @Path(downloadRegex)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFile(@QueryParam("datasetLocation") Long dsLoc) {

        try {
            String filePath = getFilePath(requestPath, dsLoc);
            File file = new File(filePath);
            return Response.ok(file, MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"") //optional
                .build();
        } catch (NullPointerException ex) {
            throw new RestException(ex, 404, "Missing file identifier", ex.getMessage());
        }
        catch (SQLException ex) {
            throw new RestException(ex, 404, "File doesn't exist", ex.getMessage());
        } catch (IllegalArgumentException ex) {
            throw new RestException(ex, 400, "Unable to download, see message", ex.getMessage());
        }
    }

    private String getFilePath(String verb, long id) throws SQLException {
        String sql = "SELECT Path from VerDatasetLocation WHERE DatasetLocation = ?";
        String filePath;
        if (!verb.equals("/get")){
            throw new IllegalArgumentException("Invalid request");
        }
        Connection conn = getConnection();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()){
                filePath = rs.getString("Path");
            } else {
                throw new SQLException("Invalid file identifier");
            }
        }
        conn.close();
        return filePath;
    }
}
