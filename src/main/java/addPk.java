import com.alibaba.druid.pool.DruidDataSource;
import org.zht.Utils.DruidDSUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;

public class addPk {
    public static void main(String[] args) {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        Connection conn ;
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
                String checkSql =
                        " select count(1) fg from(" +
                                "SELECT  (CASE  WHEN (SELECT COUNT(*) FROM pg_constraint AS PC WHERE b.attnum = PC.conkey[1] AND PC.contype = 'p'  and  PC.conname = '"+ tablename+ "'||'_pkey' ) > 0 THEN 'PRI' ELSE '' END)  AS key " +
                                "FROM information_schema.columns AS col " +
                                "         LEFT JOIN pg_namespace ns ON ns.nspname = col.table_schema " +
                                "         LEFT JOIN pg_class c ON col.table_name = c.relname AND c.relnamespace = ns.oid " +
                                "         LEFT JOIN pg_attribute b ON b.attrelid = c.oid AND b.attname = col.column_name " +
                                "WHERE col.table_schema = 'public'   AND col.table_name = '"+tablename+"'  ) n where n.key <>'' ";

                ResultSet checkResult = conn.prepareStatement(checkSql).executeQuery();
                while(checkResult.next()){
                    if(checkResult.getInt("fg")>0){
                        System.out.println(tablename+"有主键了");
                        //说明这个表主键,不用设置了
                    }else{
                        //剩下的都是没有主键的表
                        conn.setAutoCommit(false);
                        String tableToOld ="bak_"+tablename;
                        String renameSql = "alter table "+tablename+" rename to "+tableToOld+" ";//修改原表名字
                        conn.prepareStatement(renameSql).execute();
                        String copyTable = " CREATE TABLE "+tablename+" (LIKE "+tableToOld+" INCLUDING ALL); ";
                        conn.prepareStatement(copyTable).execute(); //复制了这张表
                        //给表加一列
                        String addcolumnSql = " ALTER table "+tablename+" add uuid int8";
                        conn.prepareStatement(addcolumnSql).execute();
                        String setPk ="ALTER TABLE  "+tablename+"  ADD PRIMARY KEY (uuid)";
                        conn.prepareStatement(setPk).execute();

                        //添加自增序列
                        String autoAdd = " CREATE SEQUENCE IF NOT EXISTS  "+tablename+"_id_seq  START WITH 1  INCREMENT BY 1  NO MINVALUE  NO MAXVALUE   CACHE 1";
                        conn.prepareStatement(autoAdd).execute();
                         //设置主键自增
                        String addsql =" alter table "+tablename+" alter column uuid set default nextval('"+tablename+"_id_seq')";
                        conn.prepareStatement(addsql).execute();

                        //把旧表的数据
                        String insertSql = "insert into "+tablename+" select *,row_number() over() as uuid from( select distinct * from "+tableToOld+" ) n ";
                        conn.prepareStatement(insertSql).execute();
                        conn.commit();
                        System.out.println(tablename+"设置了主键哦");
                    }

                }

            }
        } catch (SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        }


    }
}
