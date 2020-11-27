
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

/**
 * @author 王继昌
 * @create 2020-10-13 14:51
 */
public class Tickerphoenixconnect {
    public Connection connection ;
    @Before
    public void init(){
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
        try {
            connection = DriverManager.getConnection(url,properties);
            System.out.println(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void connectclose(){
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test01(){
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from test.teacher where id = '1001'");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String id = resultSet.getString(1);
                String name = resultSet.getString(2);
                Long age = resultSet.getLong(3);
                System.out.println("id:"+id);
                System.out.println("name:"+name);
                System.out.println("age:"+age);
            }
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
