/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import static org.testng.Assert.assertEquals;

import java.security.Permission;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that Client.main() exits with code 1 (not 0) when argument parsing fails.
 *
 * <p>Before this fix, all argument parsing errors called System.exit(0), causing DSI
 * and Evergreen to mark a failed YCSB invocation as successful.
 */
public class TestClientArgParsing {

  /**
   * Thrown by NoExitSecurityManager to capture the exit code instead of
   * actually terminating the JVM.
   */
  static class ExitException extends SecurityException {
    final int status;
    ExitException(int status) {
      this.status = status;
    }
  }

  /**
   * SecurityManager that intercepts System.exit() and throws ExitException instead.
   */
  static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow everything except exit
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow everything except exit
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  @BeforeMethod
  public void installSecurityManager() {
    System.setSecurityManager(new NoExitSecurityManager());
  }

  @AfterMethod
  public void removeSecurityManager() {
    System.setSecurityManager(null);
  }

  private int runMain(String... args) {
    try {
      Client.main(args);
      return 0; // should not reach here
    } catch (ExitException e) {
      return e.status;
    }
  }

  @Test
  public void unknownOptionExitsOne() {
    assertEquals(runMain("-unknownoption"), 1);
  }

  @Test
  public void positionalArgWhereSpecifierExpectedExitsOne() {
    // Reproduces the exact failure from PERF-5917: a continuation line caused
    // "-P" to appear as a positional value instead of a flag specifier.
    assertEquals(runMain("-db", "site.ycsb.BasicDB", "-s", "-P"), 1);
  }

  @Test
  public void noArgsExitsOne() {
    assertEquals(runMain(), 1);
  }

  @Test
  public void missingThreadsValueExitsOne() {
    assertEquals(runMain("-threads"), 1);
  }

  @Test
  public void missingDbValueExitsOne() {
    assertEquals(runMain("-db"), 1);
  }
}
