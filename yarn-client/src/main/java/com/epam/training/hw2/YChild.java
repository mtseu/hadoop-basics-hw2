package com.epam.training.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.tidy.Tidy;
import org.xml.sax.Attributes;
import org.xml.sax.HandlerBase;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by miket on 3/30/16.
 */
public class YChild {

    private static final String XSLT =
    "<xsl:stylesheet xmlns:x=\"http://www.w3.org/1999/xhtml\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"><xsl:output method=\"text\"/>"
            + "<xsl:template match=\"x:head|x:script|x:style|x:option\"/>"
            + "</xsl:stylesheet>";

    private static Transformer tr = null;

    private static Set<String> stopWords = new HashSet<String>() {{
        add("and");
        add("the");
        add("a");
        add("you");
        add("we");
        add("are");
        add("login");
        add("usd");
        add("with");
        add("because");
        add("perhaps");
        add("who");
        add("what");
        add("which");
        add("that");
        add("for");
        add("all");
        add("this");
        add("per");
        add("under");
        add("our");
        add("your");
    }};

    private static List<String> readAll (FileSystem fs, Path file) throws Exception {
        List<String> lines = new ArrayList<String>();
        try (FSDataInputStream is = fs.open(file)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((is)));

            String line = reader.readLine();

            while (line != null) {
                lines.add(line);
                line = reader.readLine();
            }
        }

        return lines;
    }

    private static void processLines(List<String> lines, int offset, int linesToRead) {
        for(int i = offset; i < offset + linesToRead; i++) {
            if (i == 0) // skip header
            {
                continue;
            }

            String[] fields = lines.get(i).split("\t");

            try {
                fields[1] = buildTagsField(processLine(fields[5]));
            } catch (Exception ex) {
                ex.printStackTrace(System.err);
                continue;
            }

            lines.set(i,
                    fields[0] + "\t" +
                    fields[1] + "\t" +
                    fields[2] + "\t" +
                    fields[3] + "\t" +
                    fields[4] + "\t" +
                    fields[5] + "\t"
            );
        }
    }

    private static String buildTagsField(List<String> tags) {
        String delim = "";
        StringBuilder res = new StringBuilder();

        for(String tag: tags) {
            res.append(delim).append(tag);
            delim = ",";
        }

        return res.toString();
    }

    private static String normalize(URLConnection conn) throws Exception {
        long start = System.currentTimeMillis();
        try (InputStream input = conn.getInputStream()) {
            Tidy tidy = new Tidy();
            tidy.setXHTML(true);
            tidy.setForceOutput(true);
            tidy.setNumEntities(true);
            tidy.setShowErrors(0);
            tidy.setQuiet(true);
            Writer output = new StringWriter();

            tidy.parse(new InputStreamReader(input), output);

            System.out.println("Normalize time:" + (System.currentTimeMillis()-start));
            return output.toString();
        }
    }

    private static String extractText(String xhtml) throws Exception {
        final long start = System.currentTimeMillis();
        Writer transformed = new StringWriter();

        tr.transform(
                new StreamSource(new StringReader(xhtml.substring(xhtml.indexOf("<html")))),
                new StreamResult(transformed));

        System.out.println("Size is :" + xhtml.length());
        System.out.println("Extract text time:" + (System.currentTimeMillis()-start));
        return transformed.toString();
    }

    private static Map<String, Integer> count(String text) throws Exception {
        long start = System.currentTimeMillis();
        Map<String, Integer> map = new HashMap<String, Integer>();

        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(text));
        while(tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
            if(tokenizer.ttype == StreamTokenizer.TT_WORD) {
                if(!stopWords.contains(tokenizer.sval.toLowerCase())
                && tokenizer.sval.length() > 2) {
                    Integer cnt = map.get(tokenizer.sval);
                    if (cnt == null) {
                        cnt = 0;
                    }

                    map.put(tokenizer.sval, cnt + 1);
                }
            }
        }

        System.out.println("Count :" + (System.currentTimeMillis()-start));
        System.out.println("Found words:" + map.size());
        return map;
    }

    private static List<String> processLine(String site) throws Exception {
        URL url = new URL(site);
        List<String> res = new ArrayList<>(10);

        HttpURLConnection conn = null;
        try {
            Thread.sleep(((int)(3000 + 10000*Math.random())));
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.addRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36");

            System.out.println(conn.getResponseCode() + ":Loaded URL:" + site);

            if (conn.getResponseCode() != 200) {
                throw new Exception(conn.getResponseCode() + "Can't load the page " + url);
            }

            Map<String, Integer> counts = count(extractText(normalize(conn)));

            SortedSet<Map.Entry<String,Integer> > set = new TreeSet<Map.Entry<String, Integer>>(
                    new Comparator<Map.Entry<String, Integer>>() {
                        @Override
                        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                            if(o1.getValue() > o2.getValue()) {
                                return -1;
                            } else {
                                return 1;
                            }

                        }
                    }
            );

            long start = System.currentTimeMillis();
            for(Map.Entry<String, Integer> entry: counts.entrySet()) {
                set.add(entry);
            }
            System.out.println("Sort time:" + (System.currentTimeMillis()-start));

            int top10 = 0;
            for(Map.Entry<String, Integer> entry: set) {
                res.add(entry.getKey());
                top10++;
                if(top10 == 10) {
                    break;
                }
            }

        } catch (Exception ex) {
            if(conn != null) {
                conn.disconnect();
            }
            throw ex;
        }

        return res;

    }

    private static void storeLines(List<String> lines, int offset, int linesToWrite, Path file, FileSystem fs) throws Exception {
        long start = System.currentTimeMillis();
        try (FSDataOutputStream os = fs.create(file, true); PrintWriter writer = new PrintWriter(os)) {
            for(int i  = offset; i < offset+linesToWrite; i++) {
                writer.println(lines.get(i));
            }
        }
        System.out.println("Store :" + (System.currentTimeMillis()-start));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Child:" + Arrays.toString(args));
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        if(args.length == 0) {
            args = new String[] { "3", "2", "/opt/yarn/src-file", "/opt/yarn/stage" };
        }

        int total = Integer.valueOf(args[0]);
        int num = Integer.valueOf(args[1]);
        String src = args[2];
        String stage = args[3];

        tr = TransformerFactory.newInstance().newTransformer(
                new StreamSource(new StringReader(XSLT))
        );

        tr.setURIResolver(null);

        System.out.println("Transformer class:" + tr.getClass());
        System.out.println("Transformer props:" + tr.getOutputProperties());

        FileSystem fs = FileSystem.get(new Configuration());
        List<String> lines = readAll(fs, new Path(src));

        int linesToRead = lines.size() / total;
        int offset = num * linesToRead;
        if((num + 1) == total) {
            linesToRead += (lines.size() % total);
        }
        System.out.println("Lines to read " + linesToRead + " from offset:" + offset);

        processLines(lines, offset, linesToRead);
        storeLines(lines, offset, linesToRead, new Path(stage, "tagged" + num + ".txt"), fs);

    }

}
