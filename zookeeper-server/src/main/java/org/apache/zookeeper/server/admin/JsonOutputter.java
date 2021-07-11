package org.apache.zookeeper.server.admin;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

public class JsonOutputter implements CommandOutputter {
    static final Logger LOG = LoggerFactory.getLogger(JsonOutputter.class);

    public static final String ERROR_RESPONSE = "{\"error\": \"Exception writing command response to JSON\"}";

    private ObjectMapper mapper;

    public JsonOutputter() {
        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @Override
    public String getContentType() {
        return "application/json";
    }

    @Override
    public void output(CommandResponse response, PrintWriter pw) {
        try {
            mapper.writeValue(pw, response.toMap());
        } catch (JsonGenerationException e) {
            LOG.warn("Exception writing command response to JSON:", e);
            pw.write(ERROR_RESPONSE);
        } catch (JsonMappingException e) {
            LOG.warn("Exception writing command response to JSON:", e);
            pw.write(ERROR_RESPONSE);
        } catch (IOException e) {
            LOG.warn("Exception writing command response to JSON:", e);
            pw.write(ERROR_RESPONSE);
        }
    }

}
