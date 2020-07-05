package org.wayneyu.chat.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HBaseService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseService.class);

    private Connection connection;

    @PostConstruct
    public void init() {
        Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();

        try {
            HBaseAdmin.checkHBaseAvailable(config);

            logger.info("HBase is available");

            connection = ConnectionFactory.createConnection(config);

            try (Admin admin = connection.getAdmin()) {
                createTable(TableName.valueOf("table1"), new String[]{"default"});
            }

        } catch (IOException | ServiceException e) {
            logger.error("Couldn't connect to HBase instance", e);
            throw new RuntimeException(e);
        }
    }

    public void createTable(TableName tableName, String[] columnFamilies) throws IOException {
        if (!connection.getAdmin().tableExists(tableName)) {

            logger.info("No table '{}' found. Creating one...", tableName);

            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String cf: columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
            }
            connection.getAdmin().createTable(tableDescriptor);

            logger.info("Created table {}", tableDescriptor);
        }
    }

    public void putRow(String tableName, Put put) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
    }

    public void putRows(String tableName, List<Put> puts) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(puts);
    }

    public Result getRow(String tableName, String rowKey, String... columnFamilies) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        for (String cf: columnFamilies) {
            get.addFamily(Bytes.toBytes(cf));
        }
        return table.get(get);
    }

    public List<Result> getRowsBetween(String tableName, String columnFamily, String startRowKey, String endRowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey));
        scan.addFamily(Bytes.toBytes(columnFamily));
        ResultScanner scanner = table.getScanner(scan);
        List<Result> results = new LinkedList<>();
        Result r = scanner.next();
        while (r != null) {
            results.add(r);
            r = scanner.next();
        }
        return results;
    }

    public void deleteTable(String tableName) throws IOException {
        connection.getAdmin().disableTable(TableName.valueOf(tableName));
        connection.getAdmin().deleteTable(TableName.valueOf(tableName));
    }

    public void checkAndPut(String tableName, String rowKey, String columnFamily, String qualifier, CompareFilter.CompareOp compareOp, byte[] value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        byte[] rk = Bytes.toBytes(rowKey);
        byte[] cf = Bytes.toBytes(columnFamily);
        byte[] q = Bytes.toBytes(qualifier);
        Put put = new Put(rk);
        put.addColumn(cf, q, value);
        table.checkAndPut(rk, cf, q, compareOp, value, put);
    }

}