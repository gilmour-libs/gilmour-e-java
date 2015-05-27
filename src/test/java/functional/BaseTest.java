package functional;

import gilmour.Redis;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Created by aditya@datascale.io on 27/05/15.
 */
public class BaseTest {
    protected Redis redis;

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
        redis = new Redis();
        redis.start();
    }

    @AfterClass
    public void tearDown() {
    }
}
