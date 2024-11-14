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
}
