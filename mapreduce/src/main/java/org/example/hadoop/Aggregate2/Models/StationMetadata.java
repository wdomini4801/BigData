package org.example.hadoop.Aggregate2.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StationMetadata implements Writable {
  private String stationId;
  private String internationalStationId;
  private String latitude;
  private String longitude;

  public StationMetadata() {
  }

  public StationMetadata(String stationId, String internationalStationId,
      String latitude, String longitude) {
    this.stationId = stationId;
    this.internationalStationId = internationalStationId;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(stationId != null ? stationId : "");
    out.writeUTF(internationalStationId != null ? internationalStationId : "");
    out.writeUTF(latitude != null ? latitude : "");
    out.writeUTF(longitude != null ? longitude : "");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    stationId = in.readUTF();
    internationalStationId = in.readUTF();
    latitude = in.readUTF();
    longitude = in.readUTF();
  }

  // Getters
  public String getStationId() {
    return stationId;
  }

  public String getInternationalStationId() {
    return internationalStationId;
  }

  public String getLatitude() {
    return latitude;
  }

  public String getLongitude() {
    return longitude;
  }
}
