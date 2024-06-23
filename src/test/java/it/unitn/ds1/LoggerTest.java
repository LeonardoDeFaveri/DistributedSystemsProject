package it.unitn.ds1;

import it.unitn.ds1.utils.Logger;
import org.junit.Test;

public class LoggerTest {
    @Test
    public void testLogUpdate() {
        Logger.logUpdate(1, 0,1, 10);
    }

    @Test
    public void testLogRead() {
        Logger.logRead(9, 1);
    }

    @Test
    public void testLogReadDone() {
        Logger.logReadDone(1, 10);
    }
}
