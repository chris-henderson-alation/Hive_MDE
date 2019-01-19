package QLI.configuration;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Configuration {

    private static final Logger LOGGER = Logger.getLogger(Configuration.class.getName());

    public static Map<Pattern, List<File>> gather(File directory, Pattern ... patterns) {
        ConcurrentHashMap<Pattern, List<File>> map = new ConcurrentHashMap<>();
        for (Pattern pattern : patterns) {
            map.put(pattern, new ArrayList<>());
        }
        find(directory, map);
        return map;
    }

    public static org.apache.hadoop.conf.Configuration build(List<File> files) {
        org.apache.hadoop.conf.Configuration conf = new HiveConf();
        for (InputStream stream : open(files.toArray(new File[]{}))) {
            conf.addResource(stream);
        }
        return conf;
    }

    public static InputStream[] open(File... files) {
        ArrayList<InputStream> streams = new ArrayList<>();
        for (File file : files) {
            try {
                streams.add(new FileInputStream(file));
            } catch (FileNotFoundException e) {
                LOGGER.warn(e);
            }
        }
        return streams.toArray(new InputStream[]{});
    }

    private static void find(File file, Map<Pattern, List<File>> map) {
        if (file.isFile()) {
            map.keySet().stream()
                    .filter(pattern -> pattern.matcher(file.getName()).matches())
                    .forEach(pattern -> map.get(pattern).add(file));
        } else {
            File[] files = file.listFiles();
            if (files == null) {
                return;
            }
            Arrays.stream(files).forEach(f -> find(f, map));
        }
    }
}
