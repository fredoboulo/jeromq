package perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.zeromq.ZMQ;

import zmq.Helper;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricReader
{
    private static final Comparator<MetricEntry> FIRSTSIZE = new FirstSize();
    private static final Comparator<MetricEntry> FIRSTMSGS = new FirstMessages();

    private static final class FirstSize implements Comparator<MetricEntry>
    {
        @Override
        public int compare(MetricEntry entry1, MetricEntry entry2)
        {
            int comparison = Integer.valueOf(entry1.type)
                    .compareTo(entry2.type);
            if (comparison == 0) {
                comparison = Integer.valueOf(entry1.size)
                        .compareTo(entry2.size);
            }
            if (comparison == 0) {
                comparison = FIRSTMSGS.compare(entry1, entry2);
            }
            return comparison;
        }
    }

    private static final class FirstMessages implements Comparator<MetricEntry>
    {
        @Override
        public int compare(MetricEntry entry1, MetricEntry entry2)
        {
            int comparison = Integer.valueOf(entry1.type)
                    .compareTo(entry2.type);
            if (comparison == 0) {
                comparison = Integer.valueOf(entry1.msgs)
                        .compareTo(entry2.msgs);
            }
            if (comparison == 0) {
                comparison = FIRSTSIZE.compare(entry1, entry2);
            }
            return comparison;
        }
    }

    void print(MetricRegistry registry, Map<String, Long> times)
    {
        System.out
                .printf("\n%d Tests performed. Each %s in milliseconds. Results matrix:\n",
                        times.size(), times);
        printHistos(registry);

        //        printTimers(registry);

        //        printMeters(registry);
        //        printCounters(registry);

    }

    private void printMeters(MetricRegistry registry)
    {
        SortedMap<String, Meter> meters = registry.getMeters();
        for (Entry<String, Meter> entry : meters.entrySet()) {
            String name = entry.getKey();
            Meter meter = entry.getValue();
            if (name.endsWith("msg")) {
                double oneMinuteRate = meter.getOneMinuteRate();
                double fiveMinuteRate = meter.getFiveMinuteRate();
                double fifteenMinuteRate = meter.getFifteenMinuteRate();
                double meanRate = meter.getMeanRate();
                long count = meter.getCount();
                System.out
                        .printf("Got meter %s \t\trate 1m: %.3f 5m: %.3f 15m: %.3f mean: %.3f count %d\n",
                                name, oneMinuteRate, fiveMinuteRate,
                                fifteenMinuteRate, meanRate, count);
            }
        }
    }

    private void printTimers(MetricRegistry registry)
    {
        System.out.println(" *** ** Timers ** *** ");
        List<String> sis = new ArrayList<String>();
        printHeader(sis);

        SortedMap<String, Timer> meters = registry.getTimers();
        for (Entry<String, Timer> entry : meters.entrySet()) {
            String name = entry.getKey();
            Timer timer = entry.getValue();
            if (name.endsWith("timer.recv") || name.endsWith("timer.send")) {
                Histogram histogram = registry.histogram("accu." + name);
                Snapshot snapshot = timer.getSnapshot();
                long[] values = snapshot.getValues();
                for (long val : values) {
                    histogram.update(val);
                }
            }
        }

        SortedMap<String, Histogram> histograms = registry.getHistograms();
        List<MetricEntry> ents = new ArrayList<MetricEntry>();
        Map<MetricEntry, Histogram> map = new HashMap<MetricEntry, Histogram>();
        for (Entry<String, Histogram> entry : histograms.entrySet()) {
            String name = entry.getKey();
            Histogram histogram = entry.getValue();
            if (name.startsWith("accu.")) {
                int idx = name.indexOf("msgs");
                int end = name.indexOf(".", idx + "msgs".length() + 2);
                String msgs = name.substring(idx + "msgs".length() + 1, end);
                idx = name.indexOf("size");
                end = name.indexOf(".", idx + "size".length() + 2);
                String size = name.substring(idx + "size".length() + 1, end);

                String type = name.substring(end);
                int t = 0;
                if (type.contains("send")) {
                    t = 1;
                }
                MetricEntry ent = new MetricEntry(t, Integer.parseInt(msgs),
                        Integer.parseInt(size));
                ents.add(ent);
                map.put(ent, histogram);
            }
        }
        Collections.sort(ents, FIRSTMSGS);
        for (MetricEntry ent : ents) {
            Histogram histogram = map.get(ent);
            int type = ent.type;
            int msgs = ent.msgs;
            int size = ent.size;
            //            System.out.println("Found " + msgs + " " + type + " " + size);
            String t = "recv";
            if (type == 1) {
                t = "send";
            }
            print(0, histogram.getSnapshot(), registry, 0, t + " " + msgs,
                    size, "%12.2fs");
        }
        System.out.println(" *** ** Timers ** *** ");
    }

    private void printCounters(MetricRegistry registry)
    {
        SortedMap<String, Counter> meters = registry.getCounters();
        for (Entry<String, Counter> entry : meters.entrySet()) {
            String name = entry.getKey();
            Counter counter = entry.getValue();
            if (name.endsWith("cnt")) {
                double count = counter.getCount();
                System.out.printf("Got counter %s \t\tcount %.3f\n", name,
                        count);
            }
        }
    }

    void printHistos(MetricRegistry registry)
    {
        SortedMap<String, Histogram> histograms = registry.getHistograms();

        Map<MetricEntry, Snapshot> snaps = new HashMap<MetricEntry, Snapshot>();
        for (Entry<String, Histogram> entry : histograms.entrySet()) {
            Histogram value = entry.getValue();
            String name = entry.getKey();
            Snapshot snapshot = value.getSnapshot();

            long count = value.getCount();

            MetricEntry ent = parseEntry(name);
            snaps.put(ent, snapshot);
        }

        Set<MetricEntry> keys = snaps.keySet();
        List<MetricEntry> entries = new ArrayList<MetricEntry>(keys);
        Comparator<? super MetricEntry> comparator = new FirstSize();

        Collections.sort(entries, comparator);
        List<String> sis = printMatrixTests(snaps, entries);

        Map<Integer, Histogram> allHistos = new HashMap<Integer, Histogram>();
        for (MetricEntry e : keys) {
            if (e.type == 0) {
                Histogram histogramSmall1 = registry
                        .histogram(getMetricsSeedName() + "." + "small-"
                                + e.msgs);
                allHistos.put(e.msgs, histogramSmall1);
            }
        }

        List<String> msgs = new ArrayList<String>();
        System.out.println(" *** ** * ** *** ");
        System.out.println("     Summary");
        System.out.println(" *** ** * ** *** ");
        //        System.out.print("Measures: {");
        //        for (MetricEntry entry : keys) {
        //            System.out.printf("%1d[m%sxs%s], ", entry.type, entry.msgs, entry.size);
        //        }
        //        System.out.println("}");
        //        System.out.println(" *** ** * ** *** ");
        String f = "%11.2f";
        //        printSpeed(registry, snaps, entries, sis, current, histogram,
        //                allHistos, msgs, f);

        f = printThroughput(registry, snaps, keys, entries, sis, allHistos,
                msgs, f);
        printThroughputComparison(registry, entries, sis, msgs, f);
        System.out.println();
        System.out.println(" *** ** * ** *** ");

        printDroppedMatrixTests(registry, snaps, entries);
        System.out.println();

        //        comparator = new FirstMessages();
        //        Collections.sort(entries, comparator);
        //        for (Ent entry : entries) {
        //            f = "%10.2f";
        //            print(0, snaps.get(entry), Integer.toString(entry.msgs), entry.size, f);
        //        }
        //
        //        System.out.println("================");
        //        comparator = new FirstSize();
        //        Collections.sort(entries, comparator);
        //        for (Ent entry : entries) {
        //            f = "%10.2f";
        //            print(0, snaps.get(entry), Integer.toString(entry.msgs), entry.size, f);
        //        }
    }

    private MetricEntry parseEntry(String name)
    {
        name = name.replaceAll(getMetricsSeedName(), "");

        String[] values = name.split("\\.");
        String label = values[0];
        String ind1 = values[2];
        Integer val1 = Integer.valueOf(values[3]);
        String ind2 = values[4];
        Integer val2 = Integer.valueOf(values[5]);
        int msgs = val1.intValue();
        int size = val2.intValue();

        int type = 0;
        if ("Mb_s".equals(label)) {
            type = 0;
        }
        if ("Th".equals(label)) {
            type = 1;
        }
        if ("Drops".equals(label)) {
            type = 2;
        }
        MetricEntry ent = new MetricEntry(type, msgs, size);
        return ent;
    }

    private void printThroughputComparison(MetricRegistry registry,
            List<MetricEntry> entries, List<String> sis, List<String> msgs,
            String f)
    {
        int current;
        Histogram histogram;
        current = -1;
        histogram = null;
        msgs.clear();
        System.out.println(" *** ** Throughput ZMQ Comparison ** *** ");
        printHeader(sis);
        MetricEntry cur;
        for (MetricEntry entry : entries) {
            cur = entry;
            int size = entry.size;
            int m = entry.msgs;
            if (entry.type != 1) {
                continue;
            }
            if (m < 1000
                    && !(current == 1000000 || current == 10000000
                            || current == 100000000 || current == 1000000000)) {
                continue;
            }
            if (m == 10) {
                continue;
            }
            if (current != size) {
                if (histogram != null) {
                    f = "%11.0f";
                    if (current == 10 || current == 100 || current == 1000
                            || current == 10000 || current == 100000
                            || current == 1000000 || current == 10000000
                            || current == 100000000 || current == 1000000000) {
                        print(1, histogram.getSnapshot(), registry, 0,
                                msgs.toString(), current, f);
                    }
                    msgs.clear();
                }
                histogram = registry.histogram(getMetricsSeedName() + "."
                        + "throughput-combined" + size);
                current = size;
            }
            msgs.add(format(m));
        }
        if (histogram != null) {
            print(1, histogram.getSnapshot(), registry, 0, msgs.toString(),
                    current, f);
        }
    }

    private String printThroughput(MetricRegistry registry,
            Map<MetricEntry, Snapshot> snaps, Set<MetricEntry> keys,
            List<MetricEntry> entries, List<String> sis,
            Map<Integer, Histogram> allHistos, List<String> msgs, String f)
    {
        int current;
        Histogram histogram;
        List<Integer> kk;
        System.out.println(" *** ** Throughput ** *** ");
        printHeader(sis);
        allHistos.clear();
        for (MetricEntry e : keys) {
            if (e.type == 0) {
                Histogram histogramSmall1 = registry
                        .histogram(getMetricsSeedName() + "." + "small-"
                                + e.msgs + ".throughput");
                allHistos.put(e.msgs, histogramSmall1);
            }
        }
        current = -1;
        histogram = null;
        msgs.clear();
        for (MetricEntry entry : entries) {
            int size = entry.size;
            int m = entry.msgs;
            if (entry.type != 1) {
                continue;
            }
            append(snaps, allHistos.get(m), entry);
            if (m < 20) {
                continue;
            }
            if (current != size) {
                if (histogram != null) {
                    f = "%11.0f";
                    print(1, histogram.getSnapshot(), null, 0, msgs.toString(),
                            current, f);
                    msgs.clear();
                }
                histogram = registry.histogram(getMetricsSeedName() + "."
                        + "throughput-combined" + size);
                current = size;
            }
            msgs.add(format(m));
            append(snaps, histogram, entry);
        }
        if (histogram != null) {
            print(1, histogram.getSnapshot(), registry, 0, msgs.toString(),
                    current, f);
        }
        kk = new ArrayList<Integer>(allHistos.keySet());
        Collections.sort(kk);
        for (Integer key : kk) {
            print(1, allHistos.get(key).getSnapshot(), null, key.toString(),
                    "<All>", f);
        }
        return f;
    }

    private String printSpeed(MetricRegistry registry,
            Map<MetricEntry, Snapshot> snaps, List<MetricEntry> entries,
            List<String> sis, int current, Histogram histogram,
            Map<Integer, Histogram> allHistos, List<String> msgs, String f)
    {
        System.out.println(" *** ** Speed ** *** ");
        // print header
        printHeader(sis);
        for (MetricEntry entry : entries) {
            int size = entry.size;
            int m = entry.msgs;
            if (entry.type != 0) {
                continue;
            }
            append(snaps, allHistos.get(m), entry);

            if (m == 1) {
                continue;
            }
            if (m == 10) {
                continue;
            }
            if (current != size) {
                if (histogram != null) {
                    print(0, histogram.getSnapshot(), registry, 0,
                            msgs.toString(), current, f);
                    msgs.clear();
                }
                histogram = registry.histogram(getMetricsSeedName() + "."
                        + "speed-combined" + size);
                current = size;
            }
            msgs.add(format(m));
            long[] values = append(snaps, histogram, entry);
        }

        if (histogram != null) {
            print(0, histogram.getSnapshot(), registry, 0, msgs.toString(),
                    current, f);
        }
        List<Integer> kk = new ArrayList<Integer>(allHistos.keySet());
        Collections.sort(kk);
        for (Integer key : kk) {
            print(0, allHistos.get(key).getSnapshot(), null, key.toString(),
                    "<All>", f);
        }
        return f;
    }

    private List<String> printMatrixTests(Map<MetricEntry, Snapshot> snaps,
            List<MetricEntry> entries)
    {
        List<Integer> messages = new ArrayList<Integer>();
        List<Integer> sizes = new ArrayList<Integer>();
        for (MetricEntry e : entries) {
            int msgs = e.msgs;
            int size = e.size;
            int type = e.type;
            if (type == 0) {
                if (!messages.contains(msgs)) {
                    messages.add(msgs);
                }
                if (!sizes.contains(size)) {
                    sizes.add(size);
                }
            }
        }
        Collections.sort(messages);
        Collections.sort(sizes);

        System.out.println();
        String pattern = "%9s|" + Helper.repeat(sizes.size(), "%3s")
                + "|%1$-9s";
        List<String> ms = new ArrayList<String>(messages.size());
        List<String> sis = new ArrayList<String>(sizes.size());
        sis.add("Msgs\\Size");
        for (Integer msg : sizes) {
            sis.add(format(msg.intValue()));
        }
        // print header
        // 1st line
        List<String> parts = new ArrayList<String>();
        parts = format(1, sis);
        System.out.printf(pattern + "\n", parts.toArray());
        // 2nd line
        parts = format(2, sis);
        System.out.printf(pattern + "\n", parts.toArray());
        System.out.println(Helper.repeat(9 + 9 + sizes.size() * 3, "-"));
        // print each size per line, with messages as columns
        for (Integer msg : messages) {
            String sizeDone = format(msg.intValue());
            List<String> params = new ArrayList<String>();
            params.add(sizeDone);
            ms.add(sizeDone);

            for (Integer s : sizes) {
                boolean found = false;
                int count = 0;
                for (MetricEntry k : entries) {
                    if (k.msgs == msg.intValue() && k.size == s.intValue()
                            && k.type == 0) {
                        // good, one result
                        found = true;
                        count += snaps.get(k).size();
                        //                        break;
                    }
                }
                if (found) {
                    params.add(Integer.toString(count));
                }
                else {
                    params.add(" ");
                }
            }
            System.out.printf(pattern + "\n", params.toArray());
        }

        System.out.println(Helper.repeat(9 + 9 + sizes.size() * 3, "-"));
        return sis;
    }

    private List<String> printDroppedMatrixTests(MetricRegistry registry,
            Map<MetricEntry, Snapshot> snaps, List<MetricEntry> entries)
    {
        Map<MetricEntry, Counter> drops = new HashMap<MetricEntry, Counter>();
        SortedMap<String, Counter> meters = registry.getCounters();
        for (Entry<String, Counter> entry : meters.entrySet()) {
            String name = entry.getKey();
            Counter meter = entry.getValue();
            if (name.startsWith("Drop")) {
                //                System.out.println("Meter " + name);
                MetricEntry ent = parseEntry(name);

                if (ent.type == 2) {
                    drops.put(ent, meter);
                }
            }
        }

        List<Integer> messages = new ArrayList<Integer>();
        List<Integer> sizes = new ArrayList<Integer>();
        for (MetricEntry e : entries) {
            int msgs = e.msgs;
            int size = e.size;
            int type = e.type;
            if (type == 0) {
                if (!messages.contains(msgs)) {
                    messages.add(msgs);
                }
                if (!sizes.contains(size)) {
                    sizes.add(size);
                }
            }
        }
        Collections.sort(messages);
        Collections.sort(sizes);

        System.out.println();
        String pattern = "%9s|" + Helper.repeat(sizes.size(), "%2s ")
                + "|%1$-9s";
        List<String> ms = new ArrayList<String>(messages.size());
        List<String> sis = new ArrayList<String>(sizes.size());
        sis.add("Msgs\\Size");
        for (Integer msg : sizes) {
            sis.add(format(msg.intValue()));
        }
        // print header
        // 1st line
        List<String> parts = new ArrayList<String>();
        parts = format(1, sis);
        System.out.printf(pattern + "\n", parts.toArray());
        // 2nd line
        parts = format(2, sis);
        System.out.printf(pattern + "\n", parts.toArray());

        System.out.println(Helper.repeat(9 + 9 + sizes.size() * 3, "-"));

        // print each size per line, with messages as columns
        for (Integer msg : messages) {
            String sizeDone = format(msg.intValue());
            List<String> params = new ArrayList<String>();
            params.add(sizeDone);
            ms.add(sizeDone);

            for (Integer s : sizes) {
                Counter meter = drops.get(new MetricEntry(2, msg, s));
                if (meter != null) {
                    long count = meter.getCount();
                    if (count != 0) {
                        params.add(Long.toString(count));
                        continue;
                    }
                    else {
                        params.add("..");
                        continue;
                    }
                }
                params.add("");
            }
            System.out.printf("\n" + pattern + "\n\n", params.toArray());
        }

        System.out.println(Helper.repeat(9 + 9 + sizes.size() * 2, "-"));
        return sis;
    }

    private List<String> format(int i, List<String> sis)
    {
        List<String> formatted = new ArrayList<String>();
        for (String val : sis) {
            if (val.length() > 2) {
                if (i == 1) {
                    formatted.add(val.substring(0, 2));
                }
                if (i == 2) {
                    formatted.add(val.substring(2));
                }
            }
            else {
                if (i == 1) {
                    formatted.add("");
                }
                if (i == 2) {
                    formatted.add(val);
                }
            }
        }
        return formatted;
    }

    private void printHeader(List<String> sis)
    {
        sis = new ArrayList<String>();
        sis.clear();
        sis.add("Size");

        sis.add("Drops");
        sis.add("75th");
        sis.add("95th");
        sis.add("98th");
        sis.add("99th");
        sis.add("Min");
        sis.add("Max");
        sis.add("Median");
        sis.add("Unit");
        sis.add("Mean");
        sis.add("Std Dev");
        sis.add("Measures");
        String pattern = "%34s" + Helper.repeat(sis.size(), "%11s");
        sis.add(0, "Msgs sent");
        System.out.printf(pattern + "\n", sis.toArray());
    }

    static String format(long payload)
    {
        String fmt = Long.toString(payload);
        long pref = payload;
        long remainder = 0;
        String suf = "";
        if (payload / 1000000 > 0) {
            remainder = payload % 1000000;
            if (remainder == 0) {
                pref = payload / 1000000;
                suf = "M";
            }
        }
        else if (payload / 100000 > 0) {
            remainder = payload % 100000;
            if (remainder == 0) {
                pref = payload / 100000;
                suf = "00K";
            }
        }
        else if (payload / 10000 > 0) {
            remainder = payload % 10000;
            if (remainder == 0) {
                pref = payload / 10000;
                suf = "0K";
            }
        }
        else if (payload / 1000 > 0) {
            remainder = payload % 1000;
            if (remainder == 0) {
                pref = payload / 1000;
                suf = "K";
            }
        }
        fmt = pref + suf;
        return fmt;
    }

    private long[] append(Map<MetricEntry, Snapshot> snaps,
            Histogram histogram, MetricEntry entry)
    {
        long[] values = snaps.get(entry).getValues();
        for (long v : values) {
            histogram.update(v);
        }
        return values;
    }

    private void print(int i, Snapshot snapshot, MetricRegistry registry,
            long messageCount, String msgs, int size, String f)
    {
        Meter meter = null;
        if (registry != null) {
            String testName = MetricReader.getMetricsSeedName() + ".msgs."
                    + messageCount + ".size." + size;
            meter = registry.meter(testName);
        }
        print(i, snapshot, meter, msgs, Integer.toString(size), f);
    }

    private void print(int i, Snapshot snapshot, Meter meter, String msgs,
            String size, String f)
    {
        //        Snapshot snapshot = snaps.get(entry);
        double p75th = snapshot.get75thPercentile();
        double p95th = snapshot.get95thPercentile();
        double p98th = snapshot.get98thPercentile();
        double p99th = snapshot.get99thPercentile();
        double mean = snapshot.getMean();
        double med = snapshot.getMedian();
        double std = snapshot.getStdDev();
        double max = snapshot.getMax();
        double min = snapshot.getMin();
        int count = snapshot.getValues().length;
        String s = "%s";
        String msgsPattern = "%-34s";
        String d = "%10s";
        String d6 = "%11d";
        String unit = "Mb/s";
        if (i == 1) {
            unit = "msg/s";
        }
        long drops = -1;

        if (meter != null) {
            drops = meter.getCount();
        }

        String fmt = "%2$s %3$s %3$s %5$s " + "%5$s %5$s %5$s|" + "%5$s %5$s "
                + "%5$s " + "%6$11s " + "%5$s %5$s %3$s\n";
        String format = String.format(fmt, s, msgsPattern, d, d6, f, unit);

        System.out.printf(format, msgs, size, drops, p75th, p95th, p98th,
                p99th, min, max, med, mean, std, count);
    }

    protected static String getMetricsSeedName()
    {
        String name = MetricRegistry.name(ZMQ.class);
        return name;
    }
}
