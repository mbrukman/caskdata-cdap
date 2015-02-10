/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.cli.command;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.StreamProperties;
import co.cask.common.cli.Arguments;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A CLI command for getting statistics about stream events.
 */
@Beta
public class GetStreamStatsCommand extends AbstractCommand {

  private static final int DEFAULT_LIMIT = 100;
  private static final int MAX_LIMIT = 100000;

  private final StreamClient streamClient;
  private final QueryClient queryClient;

  @Inject
  public GetStreamStatsCommand(StreamClient streamClient, QueryClient queryClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.streamClient = streamClient;
    this.queryClient = queryClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    long currentTime = System.currentTimeMillis();

    String streamId = arguments.get(ArgumentName.STREAM.toString());
    // limit limit to [1, MAX_LIMIT]
    int limit = Math.max(1, Math.min(MAX_LIMIT, arguments.getInt(ArgumentName.LIMIT.toString(), DEFAULT_LIMIT)));
    long startTime = getTimestamp(arguments.get(ArgumentName.START_TIME.toString(), "min"), currentTime);
    long endTime = getTimestamp(arguments.get(ArgumentName.END_TIME.toString(), "max"), currentTime);

    // hack to validate streamId
    StreamProperties config = streamClient.getConfig(streamId);
    if (config.getFormat().getName().equals("text")) {
      output.printf("No schema found for Stream '%s'", streamId);
      output.println();
      return;
    }

    output.printf("Analyzing %d Stream events in the time range [%d, %d]...", limit, startTime, endTime);
    output.println();
    output.println();

    // build processorMap: Hive column name -> StatsProcessor
    Map<String, Set<StatsProcessor>> processorMap = Maps.newHashMap();
    Schema streamSchema = config.getFormat().getSchema();
    for (Schema.Field field : streamSchema.getFields()) {
      Schema fieldSchema = field.getSchema();
      String hiveColumnName = cdapSchemaColumName2HiveColumnName(streamId, field.getName());
      processorMap.put(hiveColumnName, getProcessorsForType(fieldSchema.getType(), fieldSchema.getUnionSchemas()));
    }

    // get a list of stream events and calculates various statistics about the events
    String timestampCol = getTimestampHiveColumn(streamId);
    ListenableFuture<ExploreExecutionResult> resultsFuture = queryClient.execute(
      "SELECT * FROM cdap_stream_" + streamId
        + " WHERE " + timestampCol + " BETWEEN " + startTime + " AND " + endTime
        + " LIMIT " + limit);
    ExploreExecutionResult results = resultsFuture.get(1, TimeUnit.MINUTES);
    List<ColumnDesc> schema = results.getResultSchema();

    // apply StatsProcessors to every element in every row
    while (results.hasNext()) {
      QueryResult row = results.next();
      for (int i = 0; i < row.getColumns().size(); i++) {
        Object column = row.getColumns().get(i);
        ColumnDesc columnDesc = schema.get(i);
        String columnName = columnDesc.getName();
        if (isUserHiveColumn(streamId, columnName)) {
          Set<StatsProcessor> processors = processorMap.get(columnName);
          if (processors != null) {
            for (StatsProcessor processor : processors) {
              processor.process(column);
            }
          }
        }
      }
    }

    // print report
    for (ColumnDesc columnDesc : schema) {
      if (isUserHiveColumn(streamId, columnDesc.getName())) {
        String truncatedColumnName = getTruncatedColumnName(streamId, columnDesc.getName());
        output.printf("column: %s, type: %s", truncatedColumnName, columnDesc.getType());
        output.println();
        Set<StatsProcessor> processors = processorMap.get(columnDesc.getName());
        if (processors != null && !processors.isEmpty()) {
          for (StatsProcessor processor : processors) {
            processor.printReport(output);
          }
          output.println();
        } else {
          output.println("No statistics available");
          output.println();
        }
      }
    }
  }

  private String getTruncatedColumnName(String streamId, String hiveColumnName) {
    String hiveTableName = getHiveTableName(streamId);
    String hiveTablePrefix = hiveTableName + ".";
    if (hiveColumnName.startsWith(hiveTablePrefix)) {
      return hiveColumnName.substring(hiveTablePrefix.length());
    }
    return hiveColumnName;
  }

  private String getTimestampHiveColumn(String streamId) {
    return cdapSchemaColumName2HiveColumnName(streamId, "ts");
  }

  private boolean isUserHiveColumn(String streamId, String hiveColumName) {
    // TODO: hardcoded
    return !cdapSchemaColumName2HiveColumnName(streamId, "ts").equals(hiveColumName)
      && !cdapSchemaColumName2HiveColumnName(streamId, "headers").equals(hiveColumName);
  }

  private String getHiveTableName(String streamId) {
    return "cdap_stream_" + streamId;
  }

  private String cdapSchemaColumName2HiveColumnName(String streamId, String schemaColumName) {
    return getHiveTableName(streamId) + "." + schemaColumName;
  }

  private Set<StatsProcessor> getProcessorsForType(Schema.Type type, List<Schema> unionSchemas) {
    ImmutableSet.Builder<StatsProcessor> result = ImmutableSet.builder();

    boolean isBoolean = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isInt = isTypeOrInUnion(Schema.Type.INT, type, unionSchemas);
    boolean isLong = isTypeOrInUnion(Schema.Type.LONG, type, unionSchemas);
    boolean isFloat = isTypeOrInUnion(Schema.Type.FLOAT, type, unionSchemas);
    boolean isDouble = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isBytes = isTypeOrInUnion(Schema.Type.DOUBLE, type, unionSchemas);
    boolean isString = isTypeOrInUnion(Schema.Type.STRING, type, unionSchemas);

    if (isBoolean || isInt || isLong || isString || isFloat || isDouble || isBytes) {
      result.add(new CountUniqueProcessor());
    }

    if (isInt || isLong || isFloat || isDouble) {
      result.add(new HistogramProcessor());
    }

    return result.build();
  }

  private boolean isTypeOrInUnion(Schema.Type desiredType, Schema.Type type, List<Schema> unionSchemas) {
    if (desiredType.equals(type)) {
      return true;
    }

    for (Schema unionSchema : unionSchemas) {
      if (desiredType == unionSchema.getType()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String getPattern() {
    return String.format("get stream-stats <%s> [limit <%s>] [start <%s>] [end <%s>]",
                         ArgumentName.STREAM, ArgumentName.LIMIT, ArgumentName.START_TIME, ArgumentName.END_TIME);
  }

  @Override
  public String getDescription() {
    return "Gets statistics for a " + ElementType.STREAM.getPrettyName() + ". " +
      "The <" + ArgumentName.LIMIT + "> limits how many Stream events to analyze; default is " + DEFAULT_LIMIT + "." +
      "The time format for <" + ArgumentName.START_TIME + "> and <" + ArgumentName.END_TIME + "> " +
      "can be a timestamp in milliseconds or " +
      "a relative time in the form of [+|-][0-9][d|h|m|s]. " +
      "<" + ArgumentName.START_TIME + "> is relative to current time; " +
      "<" + ArgumentName.END_TIME + ">, it is relative to start time. " +
      "Special constants \"min\" and \"max\" can also be used to represent \"0\" and \"max timestamp\" respectively.";
  }

  /**
   * Processes elements within a Hive column and prints out a report about the elements visited.
   */
  private interface StatsProcessor {
    void process(Object element);
    void printReport(PrintStream printStream);
  }

  /**
   * Reports the number of unique elements found.
   */
  private static final class CountUniqueProcessor implements StatsProcessor {
    private final Set<Object> elements = Sets.newHashSet();

    @Override
    public void process(Object element) {
      if (element != null) {
        elements.add(element);
      }
    }

    @Override
    public void printReport(PrintStream printStream) {
      printStream.print("Unique elements: " + elements.size());
      printStream.println();
    }
  }


  /**
   * Reports a histogram of elements found.
   */
  private static final class HistogramProcessor implements StatsProcessor {
    // 0 -> [0, 99], 1 -> [100, 199], etc. (bucket size is BUCKET_SIZE)
    private final Multiset<Integer> buckets = HashMultiset.create();
    private static final int BUCKET_SIZE = 100;

    @Override
    public void process(Object element) {
      if (element != null && element instanceof Number) {
        Number number = (Number) element;
        int bucket = number.intValue() / BUCKET_SIZE;
        buckets.add(bucket);
      }
    }

    @Override
    public void printReport(PrintStream printStream) {
      if (!buckets.isEmpty()) {
        printStream.println("Histogram:");
        List<Integer> sortedBuckets = Lists.newArrayList(buckets.elementSet());
        Collections.sort(sortedBuckets);

        for (Integer bucket : sortedBuckets) {
          int startInclusive = bucket * BUCKET_SIZE;
          int endInclusive = startInclusive + (BUCKET_SIZE - 1);
          printStream.printf("  [%d, %d]: %d", startInclusive, endInclusive, buckets.count(bucket));
          printStream.println();
        }
      }
    }
  }
}
