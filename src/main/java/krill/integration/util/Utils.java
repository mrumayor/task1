package krill.integration.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils
{
    public static String notNull(String connectString)
    {
        // TODO refactor
        String returnString = new String();
        if(connectString != null && !connectString.isEmpty())
            returnString = connectString;
        return returnString;
    }

    public static Properties loadProperties(String filename)
    {
        Properties prop = new Properties();

        try {

            InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(filename);

            prop.load(inputStream);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;

    }

    public static void main(String... args) {
        System.out.print(Utils.loadProperties("kafka/producer-defaults.properties"));
    }
}
