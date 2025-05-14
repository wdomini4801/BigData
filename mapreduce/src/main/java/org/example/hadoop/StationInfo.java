package org.example.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class StationInfo implements Writable {
    public String lat;
    public String lon;
    public String state;
    public String city;
    public String stationId;

    public StationInfo() {}

    public StationInfo(String stationId, String lat, String lon, String state, String city) {
        this.stationId = stationId;
        this.lat = lat;
        this.lon = lon;
        this.state = state;
        this.city = city;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stationId != null ? stationId : "");
        out.writeUTF(lat != null ? lat : "");
        out.writeUTF(lon != null ? lon : "");
        out.writeUTF(state != null ? state : "");
        out.writeUTF(city != null ? city : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationId = in.readUTF();
        lat = in.readUTF();
        lon = in.readUTF();
        state = in.readUTF();
        city = in.readUTF();
    }

    @Override
    public String toString() {
        return "StationInfo{" +
                "stationId='" + stationId + '\'' +
                ", lat='" + lat + '\'' +
                ", lon='" + lon + '\'' +
                ", state='" + state + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
