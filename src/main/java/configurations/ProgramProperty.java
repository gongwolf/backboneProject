package configurations;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

public class ProgramProperty {
    public HashMap<String, String> params;
    String home_folder = System.getProperty("user.home");
    private String conf_path = "data/config/config.properties";

    public ProgramProperty(String conf_path) {
        this.conf_path = conf_path;
        params = new HashMap<String, String>();
        readProperties();
    }

    public ProgramProperty() {
        params = new HashMap<String, String>();
        readProperties();
    }

    public static void main(String args[]) {
        ProgramProperty prop = new ProgramProperty();
        prop.test();
    }

    private void test() {
    }

    private void readProperties() {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(this.conf_path);

            // load a properties file
            prop.load(input);

            Enumeration<?> emunKey = prop.propertyNames();
            while (emunKey.hasMoreElements()) {
                String key = (String) emunKey.nextElement();
                String value;
                if (key.equals("neo4jdb")) {
                    value = this.home_folder + "/" + prop.getProperty(key);
                } else {
                    value = prop.getProperty(key);
                }
                System.out.println("Key : " + key + ", Value : " + value);
                this.params.put(key, value);
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
