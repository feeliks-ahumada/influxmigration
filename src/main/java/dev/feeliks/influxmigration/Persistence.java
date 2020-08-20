package dev.feeliks.influxmigration;

import com.influxdb.client.*;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Persistence {
    private static final Logger logger = LoggerFactory.getLogger(Persistence.class);

    private Persistence() {}

    public static void toInflux(List<String> records) {
        String url = "http://localhost:8086?writeTimeout=30000&connectTimeout=60000";
        String database = "PRSA";
        String retentionPolicy = "autogen";

        InfluxDBClient influxDBClient = InfluxDBClientFactory.createV1(
                url, null, null, database, retentionPolicy);
        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writeRecords(WritePrecision.S, records);
        } catch (InfluxException influxException) {
            logger.error(influxException.getMessage());
        }
        influxDBClient.close();
    }

}
