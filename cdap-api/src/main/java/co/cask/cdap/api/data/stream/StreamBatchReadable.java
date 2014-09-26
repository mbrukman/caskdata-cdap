/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.api.data.stream;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * This class is for using a Stream as input for a MapReduce job. An instance of this class should be set in the
 * {@code MapReduceContext} of the {@code beforeSubmit} method to use the Stream as input.
 *
 * <pre>
 * {@code
 *
 * class MyMapReduce implements MapReduce {
 *    public void beforeSubmit(MapReduceContext context) {
 *      context.setInput(new StreamBatchReadable("mystream"), null);
 *    }
 * }
 * }
 * </pre>
 *
 */
public class StreamBatchReadable implements BatchReadable<Long, String> {

  private static final String START_TIME_KEY = "start";
  private static final String END_TIME_KEY = "end";
  private static final String DECODER_KEY = "decoder";

  private final String streamName;
  private final long startTime;
  private final long endTime;
  private final String decoderType;

  /**
   * Specifies to use the given stream as input of a MapReduce job. Same as calling
   * {@link #useStreamInput(co.cask.cdap.api.mapreduce.MapReduceContext, String, long, long)
   * useStreamInput(context, streamName, 0L, Long.MAX_VALUE)}
   */
  public static void useStreamInput(MapReduceContext context, String streamName) {
    useStreamInput(context, streamName, 0L, Long.MAX_VALUE);
  }

  /**
   * Specifies to use the given stream as input of a MapReduce job.
   * <p/>
   * The {@code Mapper} or {@code Reducer} class
   * that reads from the Stream needs to have the {@code KEYIN} type as {@code LongWritable}, which
   * will carry timestamp of the {@link StreamEvent}.
   * <p/>
   * The {@code VALUEIN} type can be {@code BytesWritable} or {@code Text}. For {@code Text} value type,
   * the {@link StreamEvent} body will be decoded as UTF-8 String.
   *
   * @param context The context of the MapReduce job
   * @param streamName Name of the stream
   * @param startTime Start timestamp in milliseconds (inclusive) of stream events provided to the job
   * @param endTime End timestamp in milliseconds (exclusive) of stream events provided to the job
   */
  public static void useStreamInput(MapReduceContext context, String streamName, long startTime, long endTime) {
    URI uri = createStreamURI(streamName, ImmutableMap.<String, Object>of(START_TIME_KEY, startTime,
                                                                          END_TIME_KEY, endTime));
    context.setInput(uri.toString(), null);
  }

  /**
   * Specifies to use the given stream as input of a MapReduce job, with a custom {@link StreamEventDecoder} to
   * decode {@link StreamEvent} into key/value pairs.
   *
   * @param context The context of the MapReduce job
   * @param streamName Name of the stream
   * @param startTime Start timestamp in milliseconds (inclusive) of stream events provided to the job
   * @param endTime End timestamp in milliseconds (exclusive) of stream events provided to the job
   * @param decoderType The {@link StreamEventDecoder} class for decoding {@link StreamEvent}
   */
  public static void useStreamInput(MapReduceContext context, String streamName,
                                    long startTime, long endTime, Class<? extends StreamEventDecoder> decoderType) {
    URI uri = createStreamURI(streamName, ImmutableMap.<String, Object>of(START_TIME_KEY, startTime,
                                                                          END_TIME_KEY, endTime,
                                                                          DECODER_KEY, decoderType.getName()));
    context.setInput(uri.toString(), null);
  }

  /**
   * Constructs an {@link URI} that represents the stream input.
   *
   * @param streamName Name of the stream
   * @param arguments A map containing the arguments of the URI.
   */
  private static URI createStreamURI(String streamName, Map<String, Object> arguments) {
    // It forms the query string for all key/value pairs in the arguments map
    // It encodes both key and value using the URLEncoder
    String queryString = Joiner.on('&').join(
      Iterables.transform(arguments.entrySet(), new Function<Map.Entry<String, Object>, String>() {
        @Override
        public String apply(Map.Entry<String, Object> entry) {
          try {
            return String.format("%s=%s",
                                 URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()),
                                 URLEncoder.encode(entry.getValue().toString(), Charsets.UTF_8.name()));
          } catch (UnsupportedEncodingException e) {
            // Shouldn't happen as UTF-8 should always supported.
            throw Throwables.propagate(e);
          }
        }
      }));
    return URI.create(String.format("stream://%s?%s", streamName, queryString));
  }

  /**
   * Creates a StreamBatchReadable with the given URI. The URI should be in the form
   *
   * <pre>
   * {@code
   * stream://<stream_name>[?start=<start_time>[&end=<end_time>[&decoder=<decoderClass>]]]
   * }
   * </pre>
   */
  public StreamBatchReadable(URI uri) {
    Preconditions.checkArgument("stream".equals(uri.getScheme()));
    streamName = uri.getAuthority();

    String query = uri.getQuery();
    if (query != null && !query.isEmpty()) {
      Map<String, String> parameters = Splitter.on('&').withKeyValueSeparator("=").split(query);

      startTime = parameters.containsKey(START_TIME_KEY) ? Long.parseLong(parameters.get(START_TIME_KEY)) : 0L;
      endTime = parameters.containsKey(END_TIME_KEY) ? Long.parseLong(parameters.get(END_TIME_KEY)) : Long.MAX_VALUE;
      decoderType = parameters.get(DECODER_KEY);
    } else {
      startTime = 0L;
      endTime = Long.MAX_VALUE;
      decoderType = null;
    }
  }

  /**
   * Constructs an instance with the given stream name with all time range.
   *
   * @param streamName Name of the stream.
   */
  public StreamBatchReadable(String streamName) {
    this(streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Constructs an instance with the given properties.
   *
   * @param streamName Name of the stream.
   * @param startTime Start timestamp in milliseconds.
   * @param endTime End timestamp in milliseconds.
   */
  public StreamBatchReadable(String streamName, long startTime, long endTime) {
    this(URI.create(String.format("stream://%s?%s=%d&%s=%d",
                                  streamName, START_TIME_KEY, startTime, END_TIME_KEY, endTime)));
  }

  public String getStreamName() {
    return streamName;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public String getDecoderType() {
    return decoderType;
  }

  @Override
  public List<Split> getSplits() {
    // Not used.
    return null;
  }

  @Override
  public SplitReader<Long, String> createSplitReader(Split split) {
    // Not used.
    return null;
  }
}