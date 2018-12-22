package QLI;

import java.time.LocalDate;
import java.time.temporal.ChronoField;


public class DateRange {

    private final LocalDate earliest;
    private final LocalDate latest;

    DateRange(LocalDate earliest, LocalDate latest) {
        this.earliest = earliest == null ? LocalDate.MIN : earliest;
        this.latest = latest == null ? LocalDate.MAX : latest;
    }

    boolean withinYear(int year) {
        System.out.println("year " + year);
        System.out.println("earliest " + this.earliest.getYear());
        System.out.println("latest " + this.latest.getYear());

        return this.earliest.getYear() <= year && year <= this.latest.getYear();
    }

    boolean withinMonth(int month) {
        System.out.println("month " + month);
        System.out.println("earliest " + this.earliest.getMonth().getValue());
        System.out.println("latest " + this.latest.getMonth().getValue());
        System.out.println(this.earliest.getMonth().getValue() <= month && month <= this.latest.getMonth().getValue());
        return this.earliest.getMonth().getValue() <= month && month <= this.latest.getMonth().getValue();
    }

    boolean withinDay(int day) {
        System.out.println("day " + day);
        System.out.println("earliest " + this.earliest.getDayOfMonth());
        System.out.println("latest " + this.latest.getDayOfMonth());
        return this.earliest.getDayOfMonth() <= day && day <= this.latest.getDayOfMonth();
    }

    boolean withinSecond(int second) {
        return true;
//        System.out.println("second " + second);
//        this.earliest.
//        System.out.println("earliest " + this.earliest.getLong(ChronoField.SECOND_OF_DAY));
//        System.out.println("latest " + this.latest.getLong(ChronoField.SECOND_OF_DAY));
//        return this.earliest.getLong(ChronoField.SECOND_OF_DAY) <= second && second <= this.latest.getLong(ChronoField.SECOND_OF_DAY);
    }

}
