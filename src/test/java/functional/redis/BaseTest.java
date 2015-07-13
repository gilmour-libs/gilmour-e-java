package functional.redis;

import gilmour.Gilmour;
import gilmour.backends.Redis;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Created by aditya@datascale.io@datascale.io on 27/05/15.
 */
public class BaseTest {
    protected Gilmour<Redis> gilmour;
    static final Logger logger = LogManager.getLogger();

    static class TestData {
        public String strval;
        public int intval;

        public TestData(String s, int i) {
            this.intval = i;
            this.strval = s;
        }
        public TestData() {}
    }

    @BeforeClass
    public void setUp() {
        Redis backend = new Redis();
        gilmour = new Gilmour<>(backend);
        gilmour.start();
    }

    @AfterClass
    public void tearDown() {
    }
}
