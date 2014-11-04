package hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Sample {

    private static Configuration conf;
    static HTable table;
 public Sample(String tableName, String colFams) throws IOException {
                conf = HBaseConfiguration.create();
                createTable(tableName, colFams);
                table = new HTable(conf, tableName);
    }

    @SuppressWarnings("deprecation")
	void createTable(String tableName, String colFams) throws IOException {
                HBaseAdmin hbase = new HBaseAdmin(conf);
                HTableDescriptor desc = new HTableDescriptor(tableName);
                HColumnDescriptor meta = new HColumnDescriptor(colFams.getBytes());
                desc.addFamily(meta);
                hbase.createTable(desc);
    }

    public static void addColumnEntry(String tableName, String row,
                            String colFamilyName, String colName, String values)
                            throws IOException {
                byte[] rowKey = Bytes.toBytes(row);
                Put putdata = new Put(rowKey);
                putdata.add(Bytes.toBytes(colFamilyName), Bytes.toBytes(colName),
                Bytes.toBytes(values));
                table.put(putdata);
    }

    public static void getAllRecord(String tableName, String startPartialKey,
     String endPartialKey) throws IOException {
                try {
                      Scan s;
                      if (startPartialKey == null || endPartialKey == null)
                    	  s = new Scan();
                      	else
                    	  s = new Scan(Bytes.toBytes(startPartialKey),
                    			  Bytes.toBytes(endPartialKey));
                      ResultScanner ss = table.getScanner(s);

                          HashMap<String, HashMap<String, String>> outputRec = new HashMap<String, HashMap<String, String>>();

                            String imsi = "";

                            for (Result r : ss) {

                                  HashMap<String, String> keyVal = new HashMap<String, String>();

                                        for (KeyValue kv : r.raw()) {

                                              imsi = new String(kv.getRow()).substring(10);

                                                    keyVal.put(new String(kv.getQualifier()),

                                                                         new String(kv.getValue()));

                                                    outputRec.put(imsi, keyVal);

                                                    if (keyVal.size() == 3)

                                                         System.out.println(imsi + "\t" + "Incoming minutes:"

                                                                       + keyVal.get("c1") + "\t Outcoming minutes:"

                                                                       + keyVal.get("c2") + "\t Messages:"

                                                                        + keyVal.get("c3"));

                                        }

                            }

                } finally {

                }

    }

    public static void main(String[] args) throws IOException {

                String tableName = "daterecords";

                String colFamilyNames = "i";

              new Sample(tableName, colFamilyNames);

                String fileName = "/home/user/Desktop/data";

                // This will reference one line at a time

                String line = null;

                try {

                            // FileReader reads text files in the default encoding.

                            FileReader fileReader = new FileReader(fileName);

                            // Always wrap FileReader in BufferedReader.

                            BufferedReader bufferedReader = new BufferedReader(fileReader);

                            while ((line = bufferedReader.readLine()) != null) {

                                        String[] values = line.split("\t");

                                        addColumnEntry(tableName, values[0] + "-" + values[1],

                                                                colFamilyNames, "c1", values[2]);

                                        addColumnEntry(tableName, values[0] + "-" + values[1],

                                                                colFamilyNames, "c2", values[3]);

                                        addColumnEntry(tableName, values[0] + "-" + values[1],

                                                               colFamilyNames, "c3", values[4]);

                            }

                            bufferedReader.close();

                } catch (FileNotFoundException ex) {

                            System.out.println("Unable to open file '" + fileName + "'");

                } catch (IOException ex) {

                            System.out.println("Error reading file '" + fileName + "'");

                            // Or we could just do this:

                }

                getAllRecord(tableName, "20140315", "20140316");

    }

}
