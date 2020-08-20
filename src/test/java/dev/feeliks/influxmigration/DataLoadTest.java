package dev.feeliks.influxmigration;

import org.junit.Assert;
import org.junit.Test;

public class DataLoadTest {

    @Test
    public void lineProtocolApply() {
        String line = "1,2013,3,1,0,4,4,4,7,300,77,-0.7,1023,-18.8,0,\"NNW\",4.4,\"Aotizhongxin\"";
        Assert.assertEquals("Wind,station=Aotizhongxin direction=\"NNW\",speed=4.4 1362096000", DataLoad.lineProtocol(line));
    }
}