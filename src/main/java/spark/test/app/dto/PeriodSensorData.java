package spark.test.app.dto;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

import static spark.test.app.AppConstants.*;

public class PeriodSensorData implements Serializable {

    @SerializedName(TIME_SLOT_START)
    private String timeSlotStart;
    @SerializedName(PRESENCE)
    private boolean presence;
    @SerializedName(PRESENCE_CNT)
    private int presenceCnt;
    @SerializedName(TEMP_CNT)
    private int tempCnt;
    @SerializedName(LOCATION_ID)
    private String location;
    @SerializedName(TEMP_AVG)
    private double tempAvg;
    @SerializedName(TEMP_MAX)
    private double tempMax;
    @SerializedName(TEMP_MIN)
    private double tempMin;


    public PeriodSensorData() {}


    public String getTimeSlotStart() {
        return timeSlotStart;
    }

    public void setTimeSlotStart(String timeSlotStart) {
        this.timeSlotStart = timeSlotStart;
    }

    public boolean isPresence() {
        return presence;
    }

    public void setPresence(boolean presence) {
        this.presence = presence;
    }

    public int getPresenceCnt() {
        return presenceCnt;
    }

    public void setPresenceCnt(int presenceCnt) {
        this.presenceCnt = presenceCnt;
    }

    public int getTempCnt() {
        return tempCnt;
    }

    public void setTempCnt(int tempCnt) {
        this.tempCnt = tempCnt;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public double getTempAvg() {
        return tempAvg;
    }

    public void setTempAvg(double tempAvg) {
        this.tempAvg = tempAvg;
    }

    public double getTempMax() {
        return tempMax;
    }

    public void setTempMax(double tempMax) {
        this.tempMax = tempMax;
    }

    public double getTempMin() {
        return tempMin;
    }

    public void setTempMin(double tempMin) {
        this.tempMin = tempMin;
    }

    @Override
    public String toString() {
        return "PeriodSensorData{" +
                "timeSlotStart='" + timeSlotStart + '\'' +
                ", presence=" + presence +
                ", presenceCnt=" + presenceCnt +
                ", tempCnt=" + tempCnt +
                ", location='" + location + '\'' +
                ", tempAvg=" + tempAvg +
                ", tempMax=" + tempMax +
                ", tempMin=" + tempMin +
                '}';
    }
}
