package tianjie.week2;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
public class ProcessHbase {
    private static final String TABLE_NAME = "tianjie:students";
    private static final String NAME_SPACE = "tianjie";
    private static final List<String> columnFamilyList = Arrays.asList("info", "score");
    private static final Map<String, Map<String, String>> columnFamilyMap = new HashMap<String, Map<String, String>>() {
        {
            this.put("Tom", ImmutableMap.of("info:student_id", "20210000000001", "info:class", "1", "score:understanding", "75", "score:programming", "82"));
            this.put("Jerry", ImmutableMap.of("info:student_id", "20210000000002", "info:class", "1", "score:understanding", "85", "score:programming", "67"));
            this.put("Jack", ImmutableMap.of("info:student_id", "20210000000003", "info:class", "2", "score:understanding", "80", "score:programming", "80"));
            this.put("Rose", ImmutableMap.of("info:student_id", "20210000000004", "info:class", "2", "score:understanding", "60", "score:programming", "61"));
            this.put("田杰", ImmutableMap.of("info:student_id", "G20220735030081"));
        }
    };
    public ProcessHbase(){}

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Throwable var3 = null;

        try {
            Admin admin = connection.getAdmin();
            Throwable var5 = null;

            try {
                TableName tableName = TableName.valueOf(TABLE_NAME);
                if (!Arrays.asList(admin.listNamespaces()).contains(NAME_SPACE)) {
                    NamespaceDescriptor build = NamespaceDescriptor.create(NAME_SPACE).build();
                    admin.createNamespace(build);
                }

                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }

                List<ColumnFamilyDescriptor> families = new ArrayList();
                Iterator var8 = columnFamilyList.iterator();

                while(var8.hasNext()) {
                    String familyName = (String)var8.next();
                    families.add(ColumnFamilyDescriptorBuilder.newBuilder(familyName.getBytes()).build());
                }

                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
                admin.createTable(tableDescriptor);
                Table table = connection.getTable(tableName);
                Iterator var10 = columnFamilyMap.keySet().iterator();

                while(var10.hasNext()) {
                    String rowKey = (String)var10.next();
                    Put put = new Put(Bytes.toBytes(rowKey));
                    Map<String, String> rowData = (Map)columnFamilyMap.get(rowKey);
                    Iterator var14 = rowData.keySet().iterator();

                    while(var14.hasNext()) {
                        String column = (String)var14.next();
                        String[] columnInfo = column.split(":");
                        put.addColumn(Bytes.toBytes(columnInfo[0]), Bytes.toBytes(columnInfo[1]), Bytes.toBytes((String)rowData.get(column)));
                    }

                    table.put(put);
                }

                table.close();
            } catch (Throwable var38) {
                var5 = var38;
                throw var38;
            } finally {
                if (admin != null) {
                    if (var5 != null) {
                        try {
                            admin.close();
                        } catch (Throwable var37) {
                            var5.addSuppressed(var37);
                        }
                    } else {
                        admin.close();
                    }
                }

            }
        } catch (Throwable var40) {
            var3 = var40;
            throw var40;
        } finally {
            if (connection != null) {
                if (var3 != null) {
                    try {
                        connection.close();
                    } catch (Throwable var36) {
                        var3.addSuppressed(var36);
                    }
                } else {
                    connection.close();
                }
            }

        }
    }

    public void queryTable(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes("Tom"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        Cell[] var5 = cells;
        int var6 = cells.length;

        for(int var7 = 0; var7 < var6; ++var7) {
            Cell cell = var5[var7];
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }

    }

}
