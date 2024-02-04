    import org.apache.hadoop.conf.Configuration;

    import org.apache.hadoop.hbase.*;

    import org.apache.hadoop.hbase.client.*;

    import org.apache.hadoop.hbase.util.*;



    public class TestHBase {

        public static void main(String[] args) throws Exception {

            Configuration conf = HBaseConfiguration.create();

            HBaseAdmin admin = new HBaseAdmin(conf);

            try {

            

             Connection conn = ConnectionFactory.createConnection(conf);

            Admin hAdmin = conn.getAdmin();

	    TableName table1 = TableName.valueOf("test2");

	    Table table = conn.getTable(table1)               

             Scan scan = new Scan();

                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"));



                ResultScanner scanner = table.getScanner(scan);



                for (Result result = scanner.next(); result != null; result = scanner.next())



                System.out.println("Found row : " + result);

                scanner.close();

            } finally {

                admin.close();

            }

        }

    }


