import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.util.DruidDataSourceUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DruidDataSource {
     private static DataSource dataSource;

    //读取datasouece
    static {
        Properties properties = new Properties();

        try {
             properties.load(DruidDataSourceUtils.class.getClassLoader().getResourceAsStream("druid.properties"));

             //创建连接池
            dataSource = DruidDataSourceFactory.createDataSource(properties);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataSource getDataSource(){
        return dataSource;
    }


     /*
     * 获取连接的方法
     * */
    public static Connection  getConnection() throws SQLException {
        Connection connection = getDataSource().getConnection();
        return connection;
    }
}
