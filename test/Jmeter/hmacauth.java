import java.text.SimpleDateFormat;

String method = sampler.getMethod();
String path = sampler.getUrl().getPath();
String auth_key_id = vars.get("auth_key_id");
String auth_secret_key = vars.get("auth_secret_key");
String date_header = vars.get("dateHeader");

StringBuilder fullHeader = new StringBuilder()
.append(method).append("\n")
.append(path).append("\n")
.append(date_header).append("\n");
// log.info("path = " + fullHeader.toString().replace("\n", ","));
String hmacSha1 = org.apache.commons.codec.digest.HmacUtils.hmacSha1Hex(auth_secret_key, fullHeader.toString());
String hmacAuth = "SRS:" + auth_key_id + ":" + hmacSha1;
// log.info("hmacAuth = " + hmacAuth);
vars.put("hmacAuth", hmacAuth);
