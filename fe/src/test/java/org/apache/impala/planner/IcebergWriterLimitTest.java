// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.QueryConstants;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.Test;

/**
 * Demonstrates IMPALA-12587: Iceberg DELETE and UPDATE do not respect
 * the MAX_FS_WRITERS query option.
 *
 * The test shows that when max_fs_writers is set, the HDFS INSERT sink
 * properly caps its instance count, while the IcebergBufferedDeleteSink
 * and MultiDataSink (used by UPDATE) ignore the limit entirely.
 *
 * Root cause: PlanFragment.getNumInstances() only delegates to
 * HdfsTableSink.getNumInstances() (which enforces the cap). For all
 * other sink types, it returns the uncapped planRoot_.getNumInstances().
 */
public class IcebergWriterLimitTest extends PlannerTestBase {

  private static final int MAX_WRITERS = 1;
  private static final String DB = "functional_parquet";
  private static final String TABLE = "iceberg_lineitem_multiblock";

  /**
   * Returns the explain string for a given query with max_fs_writers and mt_dop set.
   */
  private String getExplainForQuery(String query) throws ImpalaException {
    TQueryOptions options = defaultQueryOptions();
    options.setMax_fs_writers(MAX_WRITERS);
    options.setMt_dop(4);
    options.setExplain_level(TExplainLevel.EXTENDED);
    options.setNum_nodes(QueryConstants.NUM_NODES_ALL);

    TQueryCtx queryCtx = TestUtils.createQueryContext(DB,
        System.getProperty("user.name"), options);
    queryCtx.client_request.setStmt(query);

    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.disableDescTblSerialization();
    frontend_.createExecRequest(planCtx);
    return planCtx.getExplainString();
  }

  /**
   * Extracts the instance count from the fragment line containing the given sink label.
   * Looks for a pattern like "hosts=N instances=M" in the fragment header preceding
   * the sink.
   */
  private int getSinkFragmentInstances(String explain, String sinkLabel) {
    String[] lines = explain.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].contains(sinkLabel)) {
        // Walk backwards to find the PLAN FRAGMENT line
        for (int j = i - 1; j >= 0; j--) {
          if (lines[j].contains("PLAN FRAGMENT")) {
            Pattern p = Pattern.compile("instances=(\\d+)");
            Matcher m = p.matcher(lines[j]);
            if (m.find()) {
              return Integer.parseInt(m.group(1));
            }
          }
        }
      }
    }
    fail("Could not find sink fragment for: " + sinkLabel + "\nExplain:\n" + explain);
    return -1;
  }

  /**
   * Verifies that Iceberg INSERT respects max_fs_writers (it uses HdfsTableSink
   * internally, so this is the baseline/control case).
   */
  @Test
  public void testIcebergInsertRespectsMaxWriters() throws ImpalaException {
    String query = String.format(
        "INSERT INTO %s.%s SELECT * FROM %s.%s WHERE l_orderkey = 1",
        DB, TABLE, DB, TABLE);
    String explain = getExplainForQuery(query);

    int instances = getSinkFragmentInstances(explain, "WRITE TO HDFS");
    assertTrue(
        String.format("IMPALA-12587 control: Iceberg INSERT should respect "
            + "max_fs_writers=%d, but sink fragment has instances=%d.\nExplain:\n%s",
            MAX_WRITERS, instances, explain),
        instances <= MAX_WRITERS);
  }

  /**
   * Demonstrates IMPALA-12587: Iceberg DELETE ignores max_fs_writers.
   * The IcebergBufferedDeleteSink does not override getNumInstances() and
   * PlanFragment.getNumInstances() does not dispatch to it.
   */
  @Test
  public void testIcebergDeleteRespectsMaxWriters() throws ImpalaException {
    String query = String.format(
        "DELETE FROM %s.%s WHERE l_orderkey = 1", DB, TABLE);
    String explain = getExplainForQuery(query);

    int instances = getSinkFragmentInstances(explain, "BUFFERED DELETE FROM ICEBERG");
    assertTrue(
        String.format("IMPALA-12587: Iceberg DELETE sink fragment has instances=%d "
            + "but should be capped at max_fs_writers=%d.\nExplain:\n%s",
            instances, MAX_WRITERS, explain),
        instances <= MAX_WRITERS);
  }

  /**
   * Demonstrates IMPALA-12587: Iceberg UPDATE ignores max_fs_writers for
   * the delete portion. The UPDATE uses a MultiDataSink wrapping both an
   * HdfsTableSink (for inserts) and IcebergBufferedDeleteSink (for deletes).
   * PlanFragment.getNumInstances() does not recognize MultiDataSink.
   */
  @Test
  public void testIcebergUpdateRespectsMaxWriters() throws ImpalaException {
    String query = String.format(
        "UPDATE %s.%s SET l_comment = 'x' WHERE l_orderkey = 1", DB, TABLE);
    String explain = getExplainForQuery(query);

    // For UPDATE, look for MULTI DATA SINK or the WRITE TO HDFS within it
    String sinkLabel = explain.contains("MULTI DATA SINK")
        ? "MULTI DATA SINK" : "WRITE TO HDFS";
    int instances = getSinkFragmentInstances(explain, sinkLabel);
    assertTrue(
        String.format("IMPALA-12587: Iceberg UPDATE sink fragment has instances=%d "
            + "but should be capped at max_fs_writers=%d.\nExplain:\n%s",
            instances, MAX_WRITERS, explain),
        instances <= MAX_WRITERS);
  }
}
