package org.example.hadoop.Aggregate2.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MeasurementData implements Writable {
  private String timestamp;
  private String stationId;
  private String pollutantType;
  private String value;

  public MeasurementData() {
  }

  public MeasurementData(String timestamp, String stationId,
      String pollutantType, String value) {
    this.timestamp = timestamp;
    this.stationId = stationId;
    this.pollutantType = pollutantType;
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(timestamp != null ? timestamp : "");
    out.writeUTF(stationId != null ? stationId : "");
    out.writeUTF(pollutantType != null ? pollutantType : "");
    out.writeUTF(value != null ? value : "");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timestamp = in.readUTF();
    stationId = in.readUTF();
    pollutantType = in.readUTF();
    value = in.readUTF();
  }

  // Getters
  public String getTimestamp() {
    return timestamp;
  }

  public String getStationId() {
    return stationId;
  }

  public String getPollutantType() {
    return pollutantType;
  }

  public String getValue() {
    return value;
  }
}