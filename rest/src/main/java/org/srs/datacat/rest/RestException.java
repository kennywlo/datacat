
package org.srs.datacat.rest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

/**
 *
 * @author bvan
 */
public class RestException extends WebApplicationException {
    private Response response;
    
    public RestException(Exception ex, int status) {
        super(ex, Response.status(status).build());
        this.response = response(status, ex.getMessage(), getCause(ex), ex.getClass().getSimpleName(), 
                Collections.EMPTY_MAP);
    }
    
    public RestException(Exception ex, int status, String message) {
        super(ex, Response.status(status).build());
        this.response = response(status, message, null, ex.getClass().getSimpleName(), Collections.EMPTY_MAP);
    }
    
    public RestException(Exception ex, int status, String message, String cause) {
        super(ex, Response.status(status).build());
        this.response = response(status, message, cause, ex.getClass().getSimpleName(), Collections.EMPTY_MAP);
    }
    
    public RestException(Exception ex, int status, String message, String cause, 
            Map<String, Object> headers) {
        super(ex, Response.status(status).build());
        this.response = response(status, message, cause, ex.getClass().getSimpleName(), headers);
    }
    
    @Override
    public Response getResponse(){
        return response;
    }
        
    public static ErrorResponse getError(String message, String cause, String type){
        return new ErrorResponse.Builder()
                .message(message)
                .cause(cause)
                .type(type)
                .code(null)
                .build();
    }
    
    private static String getCause(Exception ex){
        StringWriter st = new StringWriter();
        PrintWriter pw = new PrintWriter(st);
        ex.printStackTrace( pw );
        return st.toString();
    }
        
    private static Response response(int status, String message, String cause, String type, 
            Map<String, Object> headers){
        
        ResponseBuilder builder = Response
                .status(status)
                .entity(getError(message, cause, type));
        for(String key: headers.keySet()){
            builder.header(key, headers.get(key));
        }
        return builder.build();
    }
    
}