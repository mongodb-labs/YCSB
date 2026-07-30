package site.ycsb.db;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteError;
import site.ycsb.Status;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Pure decision logic for MongoDB overload responses: whether an exception is
 * an overload rejection, which {@link Status} represents it, and how long to back off.
 *
 * <p>Deliberately free of driver calls and I/O so it can be unit-tested without a
 * live server. {@link MongoDbClient} owns all actual database interaction.
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
   */
  static Status statusFor(Throwable t) {
    if (!isOverload(t)) {
      return null;
    }
    if (t instanceof MongoServerException) {
      return statusForCode(((MongoServerException) t).getCode());
    }
    return OVERLOAD_OTHER;
  }

  /** True when {@code t} is a duplicate-key error. */
  static boolean isDuplicateKey(Throwable t) {
    return t instanceof MongoServerException
        && ((MongoServerException) t).getCode() == DUPLICATE_KEY_ERROR_CODE;
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
   * affects roughly twenty tasks rather than one.
   *
   * <p>The exception is a load phase that is itself the measurement. Those are
   * capped with {@code maxexecutiontime}, so retry backoff would consume the
   * measured window and depress reported insert throughput. A record-bounded load
   * has no such problem: retrying costs wall-clock and still reaches the same
   * complete dataset.
   */
  enum RetryMode {
    ENABLED_BY_DEFAULT(true, "record-bounded load phase (no maxexecutiontime)"),
    ENABLED_BY_PROPERTY(true, "forced on by " + RETRY_ENABLED_PROPERTY),
    DISABLED_BY_PROPERTY(false, "forced off by " + RETRY_ENABLED_PROPERTY),
    DISABLED_TIME_CAPPED_LOAD(false,
        "load phase is time-capped by maxexecutiontime, so it is the measurement; "
        + "retry backoff would consume the measured window");

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
   * @param maxExecutionTime value of YCSB's {@code maxexecutiontime}, or null when unset
   */
  static RetryMode retryModeForLoad(String explicitSetting, String maxExecutionTime) {
    if (explicitSetting != null && !explicitSetting.trim().isEmpty()) {
      return Boolean.parseBoolean(explicitSetting.trim())
          ? RetryMode.ENABLED_BY_PROPERTY
          : RetryMode.DISABLED_BY_PROPERTY;
    }
    return isTimeCapped(maxExecutionTime)
        ? RetryMode.DISABLED_TIME_CAPPED_LOAD
        : RetryMode.ENABLED_BY_DEFAULT;
  }

  /**
   * True when {@code maxExecutionTime} is a positive number.
   *
   * <p>Anything unparseable is treated as absent on purpose: an unresolved config
   * interpolation must not quietly switch retry off, because that would reintroduce
   * the incomplete-load failure with no visible cause.
   */
  private static boolean isTimeCapped(String maxExecutionTime) {
    if (maxExecutionTime == null || maxExecutionTime.trim().isEmpty()) {
      return false;
    }
    try {
      return Long.parseLong(maxExecutionTime.trim()) > 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }

}
