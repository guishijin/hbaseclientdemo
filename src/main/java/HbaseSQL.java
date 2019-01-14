import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Formatter;

public class HbaseSQL {

    /**
     * 测试hbase-phoenix
     * @param args
     */
    public static void main(String[] args) {
        test();
    }

    /**
     * 测试phone连接
     */
    private static void test() {
        try {
            String phoenixJDBCUrl = "jdbc:phoenix:10.1.100.102:2181";
            Connection conn = DriverManager.getConnection(phoenixJDBCUrl);

            Statement stat = conn.createStatement();
            stat.execute("upsert into GSJ.USER (ID,NAME,PASSWORD) values('javaapi','桂士金--javaapi','88888888888888--javaapi--002')");
            conn.commit();
            long starttime = System.currentTimeMillis();
            ResultSet rs = stat.executeQuery("select * from GSJ.USER");
            System.out.println("\r\n-------------------------------------------------------------------------");
            Formatter formatter = new Formatter(System.out);
            formatter.format("| %-15s | %-20s | %-20s |\n","ID","NAME","PASSWORD");
            while (rs.next()) {
                String v1 = rs.getString("ID");
                String v2 = rs.getString("NAME");
                String v3 = rs.getString("PASSWORD");
                formatter.format("| %-15s | %-20s | %-20s |\n",v1,v2,v3);
            }
            System.out.println("-------------------------------------------------------------------------\r\n");
            stat.close();
            conn.close();
            long endtime = System.currentTimeMillis();
            System.out.println("耗时："+(endtime - starttime)+" 毫秒！");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
