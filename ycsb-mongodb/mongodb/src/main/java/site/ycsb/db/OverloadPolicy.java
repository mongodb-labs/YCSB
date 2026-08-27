package site.ycsb.db;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteError;
import site.ycsb.Status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Pure decision logic for MongoDB overload responses: whether an exception is
 * an overload rejection, which {@link Status} represents it, and how long to back off.
 *
 * <p>Free of driver calls and I/O, so it can be unit-tested without a live server.
 * {@link MongoDbClient} owns all actual database interaction.
 */
final class OverloadPolicy {

  /**
   * Error label the server attaches to every error in the SystemOverloadedError
   * category. Preferred over code matching: labels are stable across releases,
   * codes drift.
   */
  static final String OVERLOAD_LABEL = "SystemOverloadedError";

  /** MongoDB duplicate-key error code. */
  static final int DUPLICATE_KEY_ERROR_CODE = 11000;

  /**
   * The complete SystemOverloadedError category, verified exhaustive against
   * src/mongo/base/error_codes.yml. Used as a fallback for per-item bulk write
   * errors, which the driver does not surface labels for.
   */
  private static final Map<Integer, Status> OVERLOAD_STATUS_BY_CODE;
  static {
    Map<Integer, Status> m = new HashMap<Integer, Status>();
    m.put(433, new Status("OVERLOAD_ADMISSION_QUEUE_OVERFLOW",
        "Rejected: admission queue overflow."));
    m.put(449, new Status("OVERLOAD_RATE_LIMIT_EXCEEDED",
        "Rejected: rate limit exceeded."));
    m.put(450, new Status("OVERLOAD_POOLED_CONNECTION_ACQUISITION_REJECTED",
        "Rejected: pooled connection acquisition rejected."));
    m.put(462, new Status("OVERLOAD_INGRESS_REQUEST_RATE_LIMIT_EXCEEDED",
        "Rejected: ingress request rate limit exceeded."));
    m.put(473, new Status("OVERLOAD_INTERRUPTED_DUE_TO_OVERLOAD",
        "Terminated: interrupted due to overload."));
    m.put(489, new Status("OVERLOAD_SEARCH_REQUEST_REJECTED_DUE_TO_OVERLOAD",
        "Rejected: search request rejected due to overload."));
    OVERLOAD_STATUS_BY_CODE = Collections.unmodifiableMap(m);
  }

  /** Fallback for a labelled error whose code is not in the table above. */
  static final Status OVERLOAD_OTHER =
      new Status("OVERLOAD_OTHER", "Rejected: server overloaded (unrecognised code).");

  static final Set<Integer> OVERLOAD_ERROR_CODES =
      Collections.unmodifiableSet(new HashSet<Integer>(OVERLOAD_STATUS_BY_CODE.keySet()));

  private OverloadPolicy() {
  }

  /**
   * True when {@code t} is a server-side overload rejection. Checks the
   * error label first, then falls back to the code table (needed for per-item
   * bulk write errors, which carry no labels).
   */
  static boolean isOverload(Throwable t) {
    if (t == null) {
      return false;
    }
    if (t instanceof MongoServerException) {
      MongoServerException mse = (MongoServerException) t;
      if (mse.hasErrorLabel(OVERLOAD_LABEL)) {
        return true;
      }
      if (OVERLOAD_ERROR_CODES.contains(mse.getCode())) {
        return true;
      }
    }
    if (t instanceof MongoBulkWriteException) {
      for (BulkWriteError err : ((MongoBulkWriteException) t).getWriteErrors()) {
        if (OVERLOAD_ERROR_CODES.contains(err.getCode())) {
          return true;
        }
      }
    }
    return false;
  }

  /** The {@link Status} representing an overload rejection with this error code. */
  static Status statusForCode(int code) {
    Status s = OVERLOAD_STATUS_BY_CODE.get(code);
    return s != null ? s : OVERLOAD_OTHER;
  }

  /**
   * The {@link Status} representing {@code t}, or {@code null} when {@code t} is
   * not an overload rejection.
   *
   * <p>The bulk case is checked first even though {@link MongoBulkWriteException}
   * extends {@link MongoServerException}: the top-level code on a bulk exception is
   * not a per-item error code, so treating it as one would collapse every batched
   * rejection into {@link #OVERLOAD_OTHER} and lose per-cause attribution. The
   * per-item codes carry the real cause.
   */
  static Status statusFor(Throwable t) {
    if (!isOverload(t)) {
      return null;
    }
    if (t instanceof MongoBulkWriteException) {
      return bulkStatus((MongoBulkWriteException) t);
    }
    if (t instanceof MongoServerException) {
      return statusForCode(((MongoServerException) t).getCode());
    }
    return OVERLOAD_OTHER;
  }

  /**
   * The {@link Status} for a batch rejection: the cause of the first recognised
   * per-item overload error.
   *
   * <p>A batch can in principle be rejected for more than one reason. Reporting the
   * first recognised cause keeps the metric one-op-one-status, which is what YCSB's
   * per-status histograms require; the alternative (a synthetic "mixed" status) would
   * create a category nobody has a threshold for.
   */
  private static Status bulkStatus(MongoBulkWriteException e) {
    for (BulkWriteError err : e.getWriteErrors()) {
      Status s = OVERLOAD_STATUS_BY_CODE.get(err.getCode());
      if (s != null) {
        return s;
      }
    }
    return OVERLOAD_OTHER;
  }

  /** True when {@code t} is a duplicate-key error. */
  static boolean isDuplicateKey(Throwable t) {
    return t instanceof MongoServerException
        && ((MongoServerException) t).getCode() == DUPLICATE_KEY_ERROR_CODE;
  }

  /**
   * How many consecutive retry rounds may be driven purely by the error-label
   * fallback before the load gives up.
   *
   * <p>The fallback exists so that a *new* overload code, added to the
   * SystemOverloadedError category after this was written, still gets retried
   * instead of hard-failing the load. But per-item bulk write errors carry no
   * labels, so the fallback has to read the label off the enclosing batch. That
   * means an ordinary per-document error (a malformed value, say) sitting in a
   * batch that was also rejected for overload looks identical to a new overload
   * code. Retrying that forever turns a one-document bug into a hung load.
   *
   * <p>A bound resolves the ambiguity in the direction that fails loudly: a real
   * overload wave clears and the counter resets, while a genuine per-document
   * error survives every round and surfaces.
   */
  static final int UNRECOGNISED_CODE_RETRY_LIMIT = 10;

  /**
   * What to do with the per-item errors of one rejected batch.
   *
   * <p>Pure data: {@link MongoDbClient} maps the indices back to documents and does
   * the sleeping. See {@link #triageBulkErrors}.
   */
  static final class BulkRetryDecision {

    private final List<Integer> retryIndices;
    private final boolean fatal;
    private final boolean usedLabelFallback;

    private BulkRetryDecision(List<Integer> retryIndices, boolean fatal,
        boolean usedLabelFallback) {
      this.retryIndices = retryIndices;
      this.fatal = fatal;
      this.usedLabelFallback = usedLabelFallback;
    }

    /** True when the batch must be propagated rather than retried. */
    boolean isFatal() {
      return fatal;
    }

    /**
     * Indices, into the list that was submitted, of the entries to retry. Empty
     * with {@link #isFatal()} false means every entry landed (all remaining errors
     * were forgiven duplicates).
     */
    List<Integer> getRetryIndices() {
      return retryIndices;
    }

    /**
     * True when at least one retry index came from the label fallback rather than
     * a recognised overload code, i.e. this round may be retrying a genuine
     * per-document error. Bounded by {@link #UNRECOGNISED_CODE_RETRY_LIMIT}.
     */
    boolean usedLabelFallback() {
      return usedLabelFallback;
    }
  }

  private static final BulkRetryDecision FATAL =
      new BulkRetryDecision(Collections.<Integer>emptyList(), true, false);

  /**
   * Classify the per-item errors of a rejected batch.
   *
   * <p>Duplicate keys are forgiven from attempt 1 onward: the write committed
   * before its rejection was reported, and YCSB load keys are deterministic and
   * unique. On attempt 0 a duplicate means the collection was not empty when the
   * load started, which is a setup problem and must surface (stock YCSB fails here
   * too).
   *
   * @param errors per-item write errors, whose {@code getIndex()} refers to the
   *     submitted list
   * @param hasOverloadLabel whether the enclosing exception carried
   *     {@link #OVERLOAD_LABEL}
   * @param attempt 0 on the first submission of this batch, incrementing per retry
   */
  static BulkRetryDecision triageBulkErrors(List<BulkWriteError> errors,
      boolean hasOverloadLabel, int attempt) {
    List<Integer> retryIndices = new ArrayList<Integer>();
    boolean usedLabelFallback = false;
    for (BulkWriteError err : errors) {
      int code = err.getCode();
      if (code == DUPLICATE_KEY_ERROR_CODE) {
        if (attempt == 0) {
          return FATAL;
        }
        continue;
      }
      if (OVERLOAD_ERROR_CODES.contains(code)) {
        retryIndices.add(err.getIndex());
        continue;
      }
      if (hasOverloadLabel) {
        usedLabelFallback = true;
        retryIndices.add(err.getIndex());
        continue;
      }
      return FATAL;
    }
    return new BulkRetryDecision(retryIndices, false, usedLabelFallback);
  }

  /** What to do with a single-document insert failure. */
  enum SingleInsertOutcome {
    /** A duplicate key on a retry: the original attempt committed. Treat as success. */
    ALREADY_COMMITTED,
    /** An overload rejection. Back off and try again. */
    RETRY,
    /** Anything else. Propagate. */
    FATAL
  }

  /**
   * Classify a single-document insert failure.
   *
   * @param t the exception thrown by the insert
   * @param attempt 0 on the first submission, incrementing per retry
   */
  static SingleInsertOutcome singleInsertOutcome(Throwable t, int attempt) {
    if (attempt > 0 && isDuplicateKey(t)) {
      return SingleInsertOutcome.ALREADY_COMMITTED;
    }
    return isOverload(t) ? SingleInsertOutcome.RETRY : SingleInsertOutcome.FATAL;
  }

  /** Initial backoff bound, in milliseconds. */
  static final long BACKOFF_BASE_MS = 100;

  /** Maximum backoff bound, in milliseconds. */
  static final long BACKOFF_MAX_MS = 5000;

  /**
   * Upper bound of the full-jitter backoff window for {@code attempt}
   * (1-based), doubling from {@link #BACKOFF_BASE_MS} and capped at
   * {@link #BACKOFF_MAX_MS}. Separated from {@link #backoffDelayMs} so the
   * schedule is testable without randomness.
   */
  static long backoffBoundMs(int attempt) {
    int safeAttempt = Math.min(Math.max(attempt, 1), 30);
    long uncapped = BACKOFF_BASE_MS * (1L << (safeAttempt - 1));
    return Math.min(BACKOFF_MAX_MS, uncapped);
  }

  /** A full-jitter delay drawn uniformly from {@code [0, backoffBoundMs(attempt)]}. */
  static long backoffDelayMs(int attempt) {
    return ThreadLocalRandom.current().nextLong(backoffBoundMs(attempt) + 1);
  }

  /** Property that forces load-phase retry on or off, overriding the default. */
  static final String RETRY_ENABLED_PROPERTY = "mongodb.overload.retry.enabled";

  /**
   * Whether the load phase retries overload rejections, and why.
   *
   * <p>Retry is on by default: an incomplete load produces a broken dataset and a
   * failing query phase, which is the whole problem PERF-8502 addresses, and it
   * affects roughly twenty tasks rather than one. Post-merge data showed the
   * backoff overhead at observed shed rates is within the noise floor, so the
   * time-capped exclusion PERF-8502 introduced was removed (PERF-9451).
   */
  enum RetryMode {
    ENABLED_BY_DEFAULT(true, "load phase (retry on by default)"),
    ENABLED_BY_PROPERTY(true, "forced on by " + RETRY_ENABLED_PROPERTY),
    DISABLED_BY_PROPERTY(false, "forced off by " + RETRY_ENABLED_PROPERTY);

    private final boolean enabled;
    private final String reason;

    RetryMode(boolean enabled, String reason) {
      this.enabled = enabled;
      this.reason = reason;
    }

    boolean isEnabled() {
      return enabled;
    }

    String reason() {
      return reason;
    }
  }

  /**
   * Decide whether the load phase should retry overload rejections.
   *
   * @param explicitSetting value of {@link #RETRY_ENABLED_PROPERTY}, or null when unset
   */
  static RetryMode retryModeForLoad(String explicitSetting) {
    if (explicitSetting != null && !explicitSetting.trim().isEmpty()) {
      return Boolean.parseBoolean(explicitSetting.trim())
          ? RetryMode.ENABLED_BY_PROPERTY
          : RetryMode.DISABLED_BY_PROPERTY;
    }
    return RetryMode.ENABLED_BY_DEFAULT;
  }

}
