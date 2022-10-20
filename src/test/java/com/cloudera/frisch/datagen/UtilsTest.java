package com.cloudera.frisch.datagen;

import com.cloudera.frisch.datagen.utils.Utils;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testFomatTimetaken() {
    long oneHourOneMinuteOneSecOneMillisec = 1 + 1000 + 1000*60 + 1000*60*60;
    assert Utils.formatTimetaken(oneHourOneMinuteOneSecOneMillisec).equalsIgnoreCase("1h 1m 1s 1ms");
  }
}
