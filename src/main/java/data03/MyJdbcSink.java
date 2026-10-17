package data03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MyJdbcSink extends RichSinkFunction<Student> {
    Connection connection = null;
    Statement statement = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        String url = "jdbc:mysql://root:3308/py_sql";
        String username = "root";
        String password = "123456";
        connection = DriverManager.getConnection(url,username,password);
        statement =  connection.createStatement();
    }

    @Override
    public void close() throws Exception {
            connection.close();
            statement.close();
    }

    @Override
    public void invoke(Student stu, Context context) throws Exception {
        statement.execute("insert into stu values(null,'"+stu.getName()+"','"+stu.getAge()+"')");
        //connection.commit();
    }
}
