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
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                Connection conn = null;
                PreparedStatement preparedStatement = null;
                try {
                    conn = dataSource.getConnection();
                    conn.createStatement();
                    String sql = "select  table_name from information_schema.columns where  table_schema='public'  group by table_name";
                    ResultSet result = conn.prepareStatement(sql).executeQuery();
                    CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<>();
                       while(result.next()){
                           String tablename = result.getString("table_name");
                            list.add(tablename);
                       }
                    System.out.println("表的数量是："+list.size());

                    for (int i = 0; i < list.size(); i++) {
                        String tablename = list.get(i);
                        String checkSql = " select count(1) n from(" +
                                "SELECT  (CASE  WHEN (SELECT COUNT(*) FROM pg_constraint AS PC WHERE b.attnum = PC.conkey[1] AND PC.contype = 'p'  and  PC.conname like concat('"+tablename+"','_%') ) > 0 THEN 'PRI' ELSE '' END)  AS key " +
                                "FROM information_schema.columns AS col " +
                                "         LEFT JOIN pg_namespace ns ON ns.nspname = col.table_schema " +
                                "         LEFT JOIN pg_class c ON col.table_name = c.relname AND c.relnamespace = ns.oid " +
                                "         LEFT JOIN pg_attribute b ON b.attrelid = c.oid AND b.attname = col.column_name " +
                                "WHERE col.table_schema = 'public'   AND col.table_name = '"+tablename+"'  ) n where n.key <>'' ";

                        ResultSet checkResult = conn.prepareStatement(checkSql).executeQuery();
                        while(checkResult.next()){
                            if(checkResult.getInt("n")>0){
                                System.out.println("表"+tablename+"有pk,可以抬出去了");
                                list.remove(i);
                            }
                        }
                    }
                    //剩下的都是没有主键的表
                    for (int i = 0; i < list.size(); i++) {
                        String tablename = list.get(i);
                        String uuidSql ="select row_number() over(order by 1) uuid,* from "+tablename+" ";
                        ResultSet resultSet = conn.prepareStatement(uuidSql).executeQuery();

//                            String truncSql ="truncate table "+tablename+" ";//清空表 然后新增列
//                            boolean execute = conn.prepareStatement(truncSql).execute();
//                            if(execute){
//                                String addSql ="ALTER TABLE "+tablename+" ADD PRIMARY KEY (uuid) ";
//                                boolean adPK = conn.prepareStatement(addSql).execute();
//                                if (adPK){
                                    System.out.println("新增主键成功,开始插入数据到原表");

                                        String columnName = resultSet.getMetaData().getColumnName(i+1);
                                        String columnType = resultSet.getMetaData().getColumnTypeName(i+1);

                                   switch (columnType){
                                       case "int8":



                                   }



                                        String value = resultSet.getString(columnName);
                                        String insertSql = "INSERT INTO "+tablename+" VALUES("+value+")";


                              //  }
                            //}



                    }
                } catch (SQLException e) {
                    //数据库连接失败异常处理
                    e.printStackTrace();
                }

            }
        };


        ThreadPoolUtil instance = ThreadPoolUtil.getInstance();
        instance.execute(runnable);
    }


}