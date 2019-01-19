import Configuration.Configuration;
import org.junit.Test;

public class TestConfiguration {

    @Test
    public void testConfiguration() throws Exception {
        Configuration conf = new Configuration("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes");
        System.out.println(conf.something());
    }
}
