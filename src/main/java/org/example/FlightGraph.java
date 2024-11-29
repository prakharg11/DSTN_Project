package org.example;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import java.util.*;

public class FlightGraph {
    private static final Map<String, Integer> delayedFlightCounts = new HashMap<>();
    private static SwingWrapper<CategoryChart> sw;
    private static CategoryChart chart;
    private static long lastUpdate = System.currentTimeMillis(); // Throttling for UI updates

    static {
        chart = new CategoryChartBuilder().width(800).height(600)
                .title("Delayed Flights vs Airlines")
                .xAxisTitle("Airlines")
                .yAxisTitle("Number of Delays")
                .build();

        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        sw = new SwingWrapper<>(chart);
        sw.displayChart();
    }

    // Method to update the graph with the current count of delayed flights per airline
    public static synchronized void updateGraph(String airline, long count) {
        if (airline == null || airline.isEmpty()) {
            System.out.println("Error: Airline is null or empty.");
            return; // Null or empty check for airline
        }

        // Update the count for this airline in the delayedFlightCounts map
        delayedFlightCounts.put(airline, (int) count);

        long now = System.currentTimeMillis();

        // Throttling updates to avoid frequent repainting (every 5 seconds)
//        if (now - lastUpdate < 5000) {
//            return; // Skip if less than 5 seconds since last update
//        }
        lastUpdate = now;

        // Convert the data to the required types for XChart
        List<String> xData = new ArrayList<>(delayedFlightCounts.keySet());
        List<Number> yData = new ArrayList<>(delayedFlightCounts.values());

        if (xData == null || yData == null || xData.isEmpty() || yData.isEmpty()) {
            System.out.println("Error: Data for the graph is empty.");
            return; // Ensure non-empty data for the graph
        }

        // Update the graph with the new data
        if (chart.getSeriesMap().containsKey("Delayed Flights")) {
            // Update the existing series with the new data
            chart.updateCategorySeries("Delayed Flights", xData, yData, null);
        } else {
            // Add a new series if it doesn't exist
            chart.addSeries("Delayed Flights", xData, yData);
        }

        // Redraw the chart
        sw.repaintChart();
    }
}
