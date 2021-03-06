package com.linkedin.camus.etl.kafka.coders;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload, and key into a JSON object,
 * wrap the key and payload into a wrapped JSON object, with additional message details.
 * look for a the camus.message.timestamp.field, convert that timestamp to
 * a unix epoch long using camus.message.timestamp.format, and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp or if the timestamp could not be parsed properly, then
 * System.currentTimeMillis() will be used.
 * <p/>
 * camus.message.timestamp.format will be used with SimpleDateFormat.  If your
 * camus.message.timestamp.field is stored in JSON as a unix epoch timestamp,
 * you should set camus.message.timestamp.format to 'unix_seconds' (if your
 * timestamp units are seconds) or 'unix_milliseconds' (if your timestamp units
 * are milliseconds).
 * <p/>
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class JsonWrappedStringMessageDecoder extends MessageDecoder<Message, String> {

    // Property for format of timestamp in JSON timestamp field.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";
    // Property for the JSON field name of the timestamp.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
    private static final Logger log = Logger.getLogger(JsonWrappedStringMessageDecoder.class);
    final JsonParser jsonParser = new JsonParser();
    final DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTimeParser();

    private String timestampFormat;
    private String timestampField;


    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;

        timestampFormat = props
                .getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
        timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
    }

    @Override
    public CamusWrapper<String> decode(Message message) {

        JsonObject messageJsonObject = new JsonObject();
        messageJsonObject.addProperty("topic", message.getTopic());
        messageJsonObject.addProperty("partition", message.getPartition());
        messageJsonObject.addProperty("offset", message.getOffset());
        messageJsonObject.addProperty("checksum", message.getChecksum());

        JsonObject payloadJsonObject = toJson(message.getPayload());
        messageJsonObject.add("payload", payloadJsonObject);

        JsonObject keyJsonObject = toJson(message.getKey());
        messageJsonObject.add("key", keyJsonObject);

        long timestamp = getTimestamp(payloadJsonObject);

        messageJsonObject.addProperty("timestamp", timestamp);

        return new CamusWrapper<>(messageJsonObject.toString(), timestamp);
    }

    private JsonObject toJson(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return toJson(toString(bytes));
    }

    private JsonObject toJson(String string) {
        if (string == null) {
            return null;
        }
        JsonObject jsonObject;// Parse the payload into a JsonObject.
        try {
            jsonObject = jsonParser.parse(string.trim()).getAsJsonObject();
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + string + "'.");
            throw new RuntimeException(e);
        }
        return jsonObject;
    }

    private String toString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        String string;

        try {
            string = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to load UTF-8 encoding, falling back to system default", e);
            string = new String(bytes);
        }
        return string;
    }

    private long getTimestamp(JsonObject jsonObject) {
        long timestamp = 0;

        // Attempt to read and parse the timestamp element into a long.
        if (jsonObject != null && jsonObject.has(timestampField)) {
            // If timestampFormat is 'unix_seconds',
            // then the timestamp only needs converted to milliseconds.
            // Also support 'unix' for backwards compatibility.
            switch (timestampFormat) {
                case "unix_seconds":
                case "unix":
                    timestamp = jsonObject.get(timestampField).getAsLong();
                    // This timestamp is in seconds, convert it to milliseconds.
                    timestamp = timestamp * 1000L;
                    break;
                // Else if this timestamp is already in milliseconds,
                // just save it as is.
                case "unix_milliseconds":
                    timestamp = jsonObject.get(timestampField).getAsLong();
                    break;
                // Else if timestampFormat is 'ISO-8601', parse that
                case "ISO-8601": {
                    String timestampString = jsonObject.get(timestampField).getAsString();
                    try {
                        timestamp = new DateTime(timestampString).getMillis();
                    } catch (IllegalArgumentException e) {
                        log.error("Could not parse timestamp '" + timestampString
                                  + "' as ISO-8601 while decoding JSON message.");
                    }
                    break;
                }
                // Otherwise parse the timestamp as a string in timestampFormat.
                default: {
                    String timestampString = jsonObject.get(timestampField).getAsString();
                    try {
                        timestamp = dateTimeParser.parseDateTime(timestampString).getMillis();
                    } catch (IllegalArgumentException e) {
                        try {
                            timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString)
                                    .getTime();
                        } catch (ParseException pe) {
                            log.error("Could not parse timestamp '" + timestampString
                                      + "' while decoding JSON message.");
                        }
                    } catch (Exception ee) {
                        log.error("Could not parse timestamp '" + timestampString
                                  + "' while decoding JSON message.");
                    }
                    break;
                }
            }
        }

        // If timestamp wasn't set in the above block,
        // then set it to current time.
        if (timestamp == 0) {
            log.warn("Couldn't find or parse timestamp field '" + timestampField
                     + "' in JSON message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
