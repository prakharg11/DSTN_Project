package org.example;

public class Flight {
    public int id;
    public int year;
    public int month;
    public int day;
    public String dep_time;
    public String sched_dep_time;
    public String dep_delay;
    public String arr_time;
    public String sched_arr_time;
    public String arr_delay;
    public String carrier;
    public String flight;
    public String tailnum;
    public String origin;
    public String dest;
    public String air_time;
    public String distance;
    public String hour;
    public String minute;
    public String time_hour;
    public String name;

    public Flight(String[] fields) {
        this.id = Integer.parseInt(fields[0]);
        this.year = Integer.parseInt(fields[1]);
        this.month = Integer.parseInt(fields[2]);
        this.day = Integer.parseInt(fields[3]);
        this.dep_time = fields[4];
        this.sched_dep_time = fields[5];
        this.dep_delay = fields[6];
        this.arr_time = fields[7];
        this.sched_arr_time = fields[8];
        this.arr_delay = fields[9];
        this.carrier = fields[10];
        this.flight = fields[11];
        this.tailnum = fields[12];
        this.origin = fields[13];
        this.dest = fields[14];
        this.air_time = fields[15];
        this.distance = fields[16];
        this.hour = fields[17];
        this.minute = fields[18];
        this.time_hour = fields[19];
        this.name = fields[20];
    }

    // Converts the Flight object to a single delimited string
    public String toDelimitedString() {
        return String.join(",",
                "id:" + id,
                "year:" + year,
                "month:" + month,
                "day:" + day,
                "dep_time:" + dep_time,
                "sched_dep_time:" + sched_dep_time,
                "dep_delay:" + dep_delay,
                "arr_time:" + arr_time,
                "sched_arr_time:" + sched_arr_time,
                "arr_delay:" + arr_delay,
                "carrier:" + carrier,
                "flight:" + flight,
                "tailnum:" + tailnum,
                "origin:" + origin,
                "dest:" + dest,
                "air_time:" + air_time,
                "distance:" + distance,
                "hour:" + hour,
                "minute:" + minute,
                "time_hour:" + time_hour,
                "name:" + name
        );
    }

    // Parses a delimited string and creates a Flight object
    public static Flight fromDelimitedString(String data) {
        String[] fields = new String[21];
        String[] keyValuePairs = data.split(",");

        for (int i = 0; i < keyValuePairs.length; i++) {
            String[] keyValue = keyValuePairs[i].split(":");
            fields[i] = keyValue.length > 1 ? keyValue[1] : "";
        }

        return new Flight(fields);
    }

    @Override
    public String toString() {
        return "Flight{" +
                "id=" + id +
                ", year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", dep_time='" + dep_time + '\'' +
                ", sched_dep_time='" + sched_dep_time + '\'' +
                ", dep_delay='" + dep_delay + '\'' +
                ", arr_time='" + arr_time + '\'' +
                ", sched_arr_time='" + sched_arr_time + '\'' +
                ", arr_delay='" + arr_delay + '\'' +
                ", carrier='" + carrier + '\'' +
                ", flight='" + flight + '\'' +
                ", tailnum='" + tailnum + '\'' +
                ", origin='" + origin + '\'' +
                ", dest='" + dest + '\'' +
                ", air_time='" + air_time + '\'' +
                ", distance='" + distance + '\'' +
                ", hour='" + hour + '\'' +
                ", minute='" + minute + '\'' +
                ", time_hour='" + time_hour + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
