import java.net.URI;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

String method = sampler.getMethod();
String path = sampler.getUrl().getPath();
String auth_key_id = vars.get("auth_key_id");
String auth_secret_key = vars.get("auth_secret_key");
String date_header = vars.get("dateHeader");
URI base_uri = new URI(vars.get("dc_server_proto")+"://"+vars.get("dc_server_host")+"/"+vars.get("dc_server_ver")+"/r");
log.info("base_path = " + base_uri.getPath());
String content_type_header = sampler.getHeaderManager().getFirstHeaderNamed("Content-Type");
String content_type="application/json";
if (content_type_header != null) {
    content_type = content_type_header.getValue();
}

path = path.replace(base_uri.getPath(), "");
static final String HMACSHA1 = "HmacSHA1";
StringBuilder fullHeader = new StringBuilder()
    .append(method).append("\n")
    .append(path).append("\n")
    .append(Objects.toString(vars.get("content-md5"), "")).append("\n")
    .append(Objects.toString(content_type, "")).append("\n")
    .append(date_header).append("\n");
log.info("path = " + fullHeader.toString().replace("\n", ","));

byte[] secretKey = DatatypeConverter.parseBase64Binary(auth_secret_key);

Mac hmac = Mac.getInstance(HMACSHA1);
SecretKey hmacKey = new SecretKeySpec(secretKey, HMACSHA1);
hmac.init(hmacKey);
byte[] rawDigest = hmac.doFinal(fullHeader.toString().getBytes());
String digest = DatatypeConverter.printBase64Binary(rawDigest);
String hmacAuth = "SRS:" + auth_key_id + ":" + digest;
// log.info("hmacAuth = " + hmacAuth);
vars.put("hmacAuth", hmacAuth);
