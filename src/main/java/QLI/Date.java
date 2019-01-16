package QLI;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Date {

    public Integer year = null;
    public Integer month = null;
    public Integer day = null;

    public static final Date MIN;
    public static final Date MAX;

    static {
        MIN = new Date(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
        MAX = new Date(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public Date(int year, int month, int day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public Date() {
    }

    public boolean before(Date other) {
        String ours = this.constructDateString(other);
        String theirs = other.constructDateString();
        return ours.compareTo(theirs) <= 0;
    }

    public boolean after(Date other) {
        String ours = this.constructDateString(other);
        String theirs = other.constructDateString();
        return ours.compareTo(theirs) >= 0;
    }

    public String constructDateString(Date other) {
        StringBuilder date = new StringBuilder();
        if (other.year != null) {
            date.append(this.year);
        }
        if (other.month != null) {
            date.append("/");
            if (this.month < 10) {
                date.append(0);
            }
            date.append(this.month);
        }
        if (other.day != null) {
            date.append("/");
            if (this.day < 10) {
                date.append(0);
            }
            date.append(this.day);
        }
        return date.toString();
    }

    public String constructDateString() {
        return this.constructDateString(this);
    }
}
