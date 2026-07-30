package site.ycsb.db;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LoadShedPolicyTest {

  /** Build a MongoCommandException carrying a code and (optionally) the overload label. */
  private static MongoCommandException commandException(int code, boolean withLabel) {
    BsonDocument response = new BsonDocument()
        .append("ok", new BsonInt32(0))
        .append("code", new BsonInt32(code))
        .append("errmsg", new BsonString("synthetic"));
    if (withLabel) {
      response.append("errorLabels",
          new BsonArray(java.util.Collections.singletonList(
              new BsonString("SystemOverloadedError"))));
    }
    return new MongoCommandException(response, new ServerAddress());
  }

  @Test
  public void labelledExceptionIsShed() {
    assertTrue(LoadShedPolicy.isShed(commandException(449, true)));
  }

  @Test
  public void knownCodeWithoutLabelIsShed() {
    // Per-item bulk write errors do not carry labels, so the code list is the fallback.
    assertTrue(LoadShedPolicy.isShed(commandException(462, false)));
  }

  @Test
  public void unrelatedErrorIsNotShed() {
    assertFalse(LoadShedPolicy.isShed(commandException(11000, false)));
  }

  @Test
  public void eachKnownCodeMapsToItsOwnStatus() {
    assertEquals(LoadShedPolicy.statusForCode(433).getName(), "SHED_ADMISSION_QUEUE_OVERFLOW");
    assertEquals(LoadShedPolicy.statusForCode(449).getName(), "SHED_RATE_LIMIT_EXCEEDED");
    assertEquals(LoadShedPolicy.statusForCode(450).getName(),
        "SHED_POOLED_CONNECTION_ACQUISITION_REJECTED");
    assertEquals(LoadShedPolicy.statusForCode(462).getName(),
        "SHED_INGRESS_REQUEST_RATE_LIMIT_EXCEEDED");
    assertEquals(LoadShedPolicy.statusForCode(473).getName(),
        "SHED_INTERRUPTED_DUE_TO_OVERLOAD");
    assertEquals(LoadShedPolicy.statusForCode(489).getName(),
        "SHED_SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD");
  }

  @Test
  public void unknownCodeMapsToShedOther() {
    // Forward compatibility: a new code under the same label must not crash or
    // be silently miscategorised.
    assertEquals(LoadShedPolicy.statusForCode(9999).getName(), "SHED_OTHER");
  }

  @Test
  public void shedStatusesAreNotOk() {
    // This is the property that makes stock YCSB route shed ops into their own
    // histograms and keeps them out of throughput.
    assertFalse(LoadShedPolicy.statusForCode(449).isOk());
    assertFalse(LoadShedPolicy.statusForCode(9999).isOk());
  }

  @Test
  public void isShedIsNullSafe() {
    assertFalse(LoadShedPolicy.isShed(null));
  }

  @Test
  public void backoffUpperBoundGrowsThenCaps() {
    // Full jitter: the delay is drawn from [0, bound]. Assert the bound, which is
    // deterministic, rather than the random draw.
    assertEquals(LoadShedPolicy.backoffBoundMs(1), 100L);
    assertEquals(LoadShedPolicy.backoffBoundMs(2), 200L);
    assertEquals(LoadShedPolicy.backoffBoundMs(3), 400L);
    assertEquals(LoadShedPolicy.backoffBoundMs(6), 3200L);
    assertEquals(LoadShedPolicy.backoffBoundMs(7), 5000L);   // capped
    assertEquals(LoadShedPolicy.backoffBoundMs(100), 5000L); // stays capped, no overflow
  }

  @Test
  public void backoffBoundIsSafeForNonPositiveAttempts() {
    assertEquals(LoadShedPolicy.backoffBoundMs(0), 100L);
    assertEquals(LoadShedPolicy.backoffBoundMs(-5), 100L);
  }

  @Test
  public void backoffDelayStaysWithinBound() {
    for (int attempt = 1; attempt <= 20; attempt++) {
      long bound = LoadShedPolicy.backoffBoundMs(attempt);
      for (int i = 0; i < 50; i++) {
        long delay = LoadShedPolicy.backoffDelayMs(attempt);
        assertTrue(delay >= 0, "delay must be non-negative, got " + delay);
        assertTrue(delay <= bound, "delay " + delay + " exceeded bound " + bound);
      }
    }
  }

  @Test
  public void retryIsOnByDefaultForARecordBoundedLoad() {
    // No explicit property, no maxexecutiontime: the ordinary setup load.
    LoadShedPolicy.RetryMode mode = LoadShedPolicy.retryModeForLoad(null, null);
    assertEquals(mode, LoadShedPolicy.RetryMode.ENABLED_BY_DEFAULT);
    assertTrue(mode.isEnabled());
  }

  @Test
  public void retryIsOffForATimeCappedLoad() {
    // ycsb.load.2024-05 and heat_4x_ycsb.load measure their load phase against a
    // clock, so backoff would eat the measured window.
    LoadShedPolicy.RetryMode mode = LoadShedPolicy.retryModeForLoad(null, "600");
    assertEquals(mode, LoadShedPolicy.RetryMode.DISABLED_TIME_CAPPED_LOAD);
    assertFalse(mode.isEnabled());
  }

  @Test
  public void aZeroTimeCapDoesNotCountAsCapped() {
    assertTrue(LoadShedPolicy.retryModeForLoad(null, "0").isEnabled());
  }

  @Test
  public void aBlankOrUnparseableTimeCapDoesNotCountAsCapped() {
    // Unresolved config interpolation must not silently disable retry.
    assertTrue(LoadShedPolicy.retryModeForLoad(null, "").isEnabled());
    assertTrue(LoadShedPolicy.retryModeForLoad(null, "   ").isEnabled());
    assertTrue(LoadShedPolicy.retryModeForLoad(null, "${notresolved}").isEnabled());
  }

  @Test
  public void explicitPropertyWinsOverTheTimeCap() {
    assertEquals(LoadShedPolicy.retryModeForLoad("true", "600"),
        LoadShedPolicy.RetryMode.ENABLED_BY_PROPERTY);
    assertTrue(LoadShedPolicy.retryModeForLoad("true", "600").isEnabled());
  }

  @Test
  public void explicitPropertyCanDisableARecordBoundedLoad() {
    assertEquals(LoadShedPolicy.retryModeForLoad("false", null),
        LoadShedPolicy.RetryMode.DISABLED_BY_PROPERTY);
    assertFalse(LoadShedPolicy.retryModeForLoad("false", null).isEnabled());
  }

  @Test
  public void everyRetryModeExplainsItself() {
    // The reason is logged at load start, so it must never be empty.
    for (LoadShedPolicy.RetryMode mode : LoadShedPolicy.RetryMode.values()) {
      assertTrue(mode.reason() != null && !mode.reason().isEmpty(), mode.name());
    }
  }
}
