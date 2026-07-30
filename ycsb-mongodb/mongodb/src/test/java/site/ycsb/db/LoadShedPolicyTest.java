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
import static org.testng.Assert.assertNull;
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
}
