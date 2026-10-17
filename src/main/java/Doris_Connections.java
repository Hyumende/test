import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Doris_Connections {
    //使用KeyedState中的ValueState获取数据中的最大值（获取每个key的最大值）(实际中直接使用maxBy即可)
    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig("/aplication.properties");

        HikariDataSource DataSource = new HikariDataSource(config);

        Connection connection = DataSource.getConnection();

        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from t1 order by pk;");

        while (resultSet.next()) {
            String string1 = resultSet.getString(1);
            String string2 = resultSet.getString(2);
            System.out.println(string1 + " " + string2);
        }

        }


    }


