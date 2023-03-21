import com.alibaba.druid.pool.DruidDataSource;
import org.zht.Utils.DruidDSUtil;
import org.zht.Utils.ThreadPoolUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;

/*
 * @Author root
 * @Data  2022/8/17 9:44
 * @Description
 * */
public class addPk {
    public static void main(String[] args) throws SQLException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        //ThreadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                Connection conn = null;
                PreparedStatement preparedStatement = null;
                try {
                    conn = dataSource.getConnection();
                    conn.createStatement();
                    String sql = "select  table_name from information_schema.columns where  table_schema='public'  group by table_name";
                    preparedStatement = conn.prepareStatement(sql);
                    ResultSet result = preparedStatement.executeQuery();
                    CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<>();
                       while(result.next()){
                           String tablename = result.getString("table_name");
                           list.add(tablename);
                       }
                    System.out.println("表的数量是："+list.size());
                    System.out.println("准备添加pk");
                    String sql2 = "";


//
//
//                    System.out.println("list的大小是" + list.size());
//                    System.out.println("数据库数据获取成功!!!");
////                    conn.close();
////                    preparedStatement.close();
//                    System.out.println("准备写入mysql");
//
//                    conn.setAutoCommit(false);

                    conn.close();
                    preparedStatement.close();
                } catch (SQLException e) {
                    //数据库连接失败异常处理
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                }
            }
        };

        ThreadPoolUtil instance = ThreadPoolUtil.getInstance();
        instance.execute(runnable);

    }


}