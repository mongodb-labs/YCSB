package site.ycsb.db;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonArray;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class OverloadPolicyTest {

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
  public void labelledExceptionIsOverload() {
    assertTrue(OverloadPolicy.isOverload(commandException(449, true)));
  }

  @Test
  public void knownCodeWithoutLabelIsOverload() {
    // Per-item bulk write errors do not carry labels, so the code list is the fallback.
    assertTrue(OverloadPolicy.isOverload(commandException(462, false)));
  }

  @Test
  public void unrelatedErrorIsNotOverload() {
    assertFalse(OverloadPolicy.isOverload(commandException(11000, false)));
  }

  @Test
  public void eachKnownCodeMapsToItsOwnStatus() {
    assertEquals(OverloadPolicy.statusForCode(433).getName(), "OVERLOAD_ADMISSION_QUEUE_OVERFLOW");
    assertEquals(OverloadPolicy.statusForCode(449).getName(), "OVERLOAD_RATE_LIMIT_EXCEEDED");
    assertEquals(OverloadPolicy.statusForCode(450).getName(),
        "OVERLOAD_POOLED_CONNECTION_ACQUISITION_REJECTED");
    assertEquals(OverloadPolicy.statusForCode(462).getName(),
        "OVERLOAD_INGRESS_REQUEST_RATE_LIMIT_EXCEEDED");
    assertEquals(OverloadPolicy.statusForCode(473).getName(),
        "OVERLOAD_INTERRUPTED_DUE_TO_OVERLOAD");
    assertEquals(OverloadPolicy.statusForCode(489).getName(),
        "OVERLOAD_SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD");
  }

  @Test
  public void unknownCodeMapsToOverloadOther() {
    // Forward compatibility: a new code under the same label must not crash or
    // be silently miscategorised.
    assertEquals(OverloadPolicy.statusForCode(9999).getName(), "OVERLOAD_OTHER");
  }

  @Test
  public void overloadStatusesAreNotOk() {
    // This is the property that makes stock YCSB route overload ops into their own
    // histograms and keeps them out of throughput.
    assertFalse(OverloadPolicy.statusForCode(449).isOk());
    assertFalse(OverloadPolicy.statusForCode(9999).isOk());
  }

  @Test
  public void isOverloadIsNullSafe() {
    assertFalse(OverloadPolicy.isOverload(null));
  }

  @Test
  public void backoffUpperBoundGrowsThenCaps() {
    // Full jitter: the delay is drawn from [0, bound]. Assert the bound, which is
    // deterministic, rather than the random draw.
    assertEquals(OverloadPolicy.backoffBoundMs(1), 100L);
    assertEquals(OverloadPolicy.backoffBoundMs(2), 200L);
    assertEquals(OverloadPolicy.backoffBoundMs(3), 400L);
    assertEquals(OverloadPolicy.backoffBoundMs(6), 3200L);
    assertEquals(OverloadPolicy.backoffBoundMs(7), 5000L);   // capped
    assertEquals(OverloadPolicy.backoffBoundMs(100), 5000L); // stays capped, no overflow
  }

  @Test
  public void backoffBoundIsSafeForNonPositiveAttempts() {
    assertEquals(OverloadPolicy.backoffBoundMs(0), 100L);
    assertEquals(OverloadPolicy.backoffBoundMs(-5), 100L);
  }

  @Test
  public void backoffDelayStaysWithinBound() {
    for (int attempt = 1; attempt <= 20; attempt++) {
      long bound = OverloadPolicy.backoffBoundMs(attempt);
      for (int i = 0; i < 50; i++) {
        long delay = OverloadPolicy.backoffDelayMs(attempt);
        assertTrue(delay >= 0, "delay must be non-negative, got " + delay);
        assertTrue(delay <= bound, "delay " + delay + " exceeded bound " + bound);
      }
    }
  }

  @Test
  public void retryIsOnByDefaultForARecordBoundedLoad() {
    // No explicit property, no maxexecutiontime: the ordinary setup load.
    OverloadPolicy.RetryMode mode = OverloadPolicy.retryModeForLoad(null, null);
    assertEquals(mode, OverloadPolicy.RetryMode.ENABLED_BY_DEFAULT);
    assertTrue(mode.isEnabled());
  }

  @Test
  public void retryIsOffForATimeCappedLoad() {
    // ycsb.load.2024-05 and heat_4x_ycsb.load measure their load phase against a
    // clock, so backoff would eat the measured window.
    OverloadPolicy.RetryMode mode = OverloadPolicy.retryModeForLoad(null, "600");
    assertEquals(mode, OverloadPolicy.RetryMode.DISABLED_TIME_CAPPED_LOAD);
    assertFalse(mode.isEnabled());
  }

  @Test
  public void aZeroTimeCapDoesNotCountAsCapped() {
    assertTrue(OverloadPolicy.retryModeForLoad(null, "0").isEnabled());
  }

  @Test
  public void aBlankOrUnparseableTimeCapDoesNotCountAsCapped() {
    // Unresolved config interpolation must not silently disable retry.
    assertTrue(OverloadPolicy.retryModeForLoad(null, "").isEnabled());
    assertTrue(OverloadPolicy.retryModeForLoad(null, "   ").isEnabled());
    assertTrue(OverloadPolicy.retryModeForLoad(null, "${notresolved}").isEnabled());
  }

  @Test
  public void explicitPropertyWinsOverTheTimeCap() {
    assertEquals(OverloadPolicy.retryModeForLoad("true", "600"),
        OverloadPolicy.RetryMode.ENABLED_BY_PROPERTY);
    assertTrue(OverloadPolicy.retryModeForLoad("true", "600").isEnabled());
  }

  @Test
  public void explicitPropertyCanDisableARecordBoundedLoad() {
    assertEquals(OverloadPolicy.retryModeForLoad("false", null),
        OverloadPolicy.RetryMode.DISABLED_BY_PROPERTY);
    assertFalse(OverloadPolicy.retryModeForLoad("false", null).isEnabled());
  }

  @Test
  public void everyRetryModeExplainsItself() {
    // The reason is logged at load start, so it must never be empty.
    for (OverloadPolicy.RetryMode mode : OverloadPolicy.RetryMode.values()) {
      assertTrue(mode.reason() != null && !mode.reason().isEmpty(), mode.name());
    }
  }

  // ── Single-document insert triage ────────────────────────────────

  @Test
  public void aFirstAttemptDuplicateIsFatal() {
    // The collection was not empty when the load started: a setup problem that must
    // surface rather than be absorbed as success.
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(11000, false), 0),
        OverloadPolicy.SingleInsertOutcome.FATAL);
  }

  @Test
  public void aDuplicateOnRetryMeansTheWriteAlreadyLanded() {
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(11000, false), 1),
        OverloadPolicy.SingleInsertOutcome.ALREADY_COMMITTED);
  }

  @Test
  public void anOverloadRejectionRetries() {
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(449, true), 0),
        OverloadPolicy.SingleInsertOutcome.RETRY);
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(449, true), 7),
        OverloadPolicy.SingleInsertOutcome.RETRY);
  }

  @Test
  public void anUnrelatedErrorIsFatalOnEveryAttempt() {
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(2, false), 0),
        OverloadPolicy.SingleInsertOutcome.FATAL);
    assertEquals(OverloadPolicy.singleInsertOutcome(commandException(2, false), 9),
        OverloadPolicy.SingleInsertOutcome.FATAL);
  }

  // ── Batch triage ─────────────────────────────────────────────────

  /** A per-item bulk write error. These never carry labels, only codes. */
  private static BulkWriteError writeError(int code, int index) {
    return new BulkWriteError(code, "synthetic", new BsonDocument(), index);
  }

  private static List<BulkWriteError> errors(BulkWriteError... e) {
    return Arrays.asList(e);
  }

  @Test
  public void recognisedOverloadCodesAreRetriedByIndex() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(449, 3), writeError(433, 7)), false, 0);

    assertFalse(d.isFatal());
    assertEquals(d.getRetryIndices(), Arrays.asList(3, 7));
    assertFalse(d.usedLabelFallback(), "recognised codes must not count as fallback");
  }

  @Test
  public void indicesReferToTheSubmittedListNotTheOriginalBatch() {
    // After the first round the caller resubmits only the failures, so the driver
    // renumbers from zero. The decision must pass those indices through untouched —
    // remapping them against the original batch would corrupt the retry set.
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(449, 0), writeError(449, 1)), false, 4);

    assertEquals(d.getRetryIndices(), Arrays.asList(0, 1));
  }

  @Test
  public void aFirstAttemptDuplicateFailsTheWholeBatch() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(449, 0), writeError(11000, 1)), true, 0);

    assertTrue(d.isFatal());
  }

  @Test
  public void duplicatesOnRetryAreForgivenAndNotResubmitted() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(11000, 0), writeError(449, 1)), false, 2);

    assertFalse(d.isFatal());
    assertEquals(d.getRetryIndices(), Arrays.asList(1));
  }

  @Test
  public void aBatchOfOnlyForgivenDuplicatesIsComplete() {
    // Nothing left to retry and nothing wrong: every write committed before its
    // rejection was reported. The caller must treat this as success, not a hang.
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(11000, 0), writeError(11000, 1)), false, 1);

    assertFalse(d.isFatal());
    assertTrue(d.getRetryIndices().isEmpty());
  }

  @Test
  public void anUnrecognisedCodeWithoutTheLabelIsFatal() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(2, 0)), false, 0);

    assertTrue(d.isFatal());
  }

  @Test
  public void anUnrecognisedCodeUnderTheLabelIsRetriedButFlagged() {
    // Forward compatibility: a new code in the SystemOverloadedError category must not
    // hard-fail the load. But per-item errors carry no labels, so this is also what a
    // genuine per-document error looks like inside an overloaded batch — hence the flag
    // that lets the caller bound it.
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(9999, 5)), true, 0);

    assertFalse(d.isFatal());
    assertEquals(d.getRetryIndices(), Arrays.asList(5));
    assertTrue(d.usedLabelFallback());
  }

  @Test
  public void oneUnrecognisedCodeFlagsTheWholeRound() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        errors(writeError(449, 0), writeError(9999, 1)), true, 3);

    assertEquals(d.getRetryIndices(), Arrays.asList(0, 1));
    assertTrue(d.usedLabelFallback());
  }

  @Test
  public void theFallbackBoundIsSmallEnoughToFailFastAndLargeEnoughToRideOutAWave() {
    // Guards the constant itself: it is the only thing standing between a malformed
    // document and a load that never terminates.
    assertTrue(OverloadPolicy.UNRECOGNISED_CODE_RETRY_LIMIT > 0);
    assertTrue(OverloadPolicy.UNRECOGNISED_CODE_RETRY_LIMIT <= 50);
  }

  @Test
  public void anEmptyErrorListRetriesNothing() {
    OverloadPolicy.BulkRetryDecision d = OverloadPolicy.triageBulkErrors(
        Collections.<BulkWriteError>emptyList(), false, 0);

    assertFalse(d.isFatal());
    assertTrue(d.getRetryIndices().isEmpty());
  }

  // ── Status attribution ───────────────────────────────────────────

  @Test
  public void aBatchRejectionReportsThePerItemCauseNotTheEnvelope() {
    // MongoBulkWriteException extends MongoServerException but its top-level code is
    // not a per-item code, so reading the cause off the envelope would collapse every
    // batched rejection into OVERLOAD_OTHER.
    MongoBulkWriteException e = bulkWriteException(writeError(462, 0));

    assertEquals(OverloadPolicy.statusFor(e).getName(),
        "OVERLOAD_INGRESS_REQUEST_RATE_LIMIT_EXCEEDED");
  }

  @Test
  public void aBatchRejectionWithNoRecognisedCauseIsOverloadOther() {
    MongoBulkWriteException e = bulkWriteException(writeError(9999, 0));
    e.addLabel("SystemOverloadedError");

    assertEquals(OverloadPolicy.statusFor(e).getName(), "OVERLOAD_OTHER");
  }

  @Test
  public void aBatchOfPlainErrorsIsNotAnOverloadStatus() {
    assertNull(OverloadPolicy.statusFor(bulkWriteException(writeError(2, 0))));
  }

  private static MongoBulkWriteException bulkWriteException(BulkWriteError... writeErrors) {
    return new MongoBulkWriteException(
        BulkWriteResult.acknowledged(0, 0, 0, 0, java.util.Collections.<BulkWriteUpsert>emptyList(),
            java.util.Collections.<BulkWriteInsert>emptyList()),
        Arrays.asList(writeErrors),
        null,
        new ServerAddress(),
        java.util.Collections.<String>emptySet());
  }
}
