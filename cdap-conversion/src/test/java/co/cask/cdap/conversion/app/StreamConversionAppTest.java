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

package co.cask.cdap.conversion.app;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StreamConversionAppTest extends TestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  @Test
  public void testMapReduce() throws Exception {
    String streamName = "test";
    String filesetName = "converted_stream";

    ApplicationManager appManager = deployApplication(StreamConversionApp.class);

    // create and write to the stream
    StreamWriter streamWriter = appManager.getStreamWriter(streamName);
    streamWriter.createStream();
    Map<String, String> headers1 = Maps.newHashMap();
    headers1.put("header1", "bar");
    String body1 = "AAPL,10,500.32";
    Map<String, String> headers2 = Maps.newHashMap();
    headers2.put("header1", "baz");
    headers2.put("header2", "foo");
    String body2 = "CASK,50,12345.67";
    streamWriter.send(headers1, body1);
    streamWriter.send(headers2, body2);
    streamWriter.send(Maps.<String, String>newHashMap(), "more content");

    /*Schema bodySchema = Schema.recordOf(
      "eventBody",
      Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("num_traded", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.FLOAT)));*/
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("header1", Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("header2", Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("data", Schema.of(Schema.Type.STRING)));
    addDatasetInstance("timePartitionedFileSet", filesetName, FileSetProperties.builder()
      .setBasePath(filesetName)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setOutputProperty("schema", schema.toString())
      .setExploreEnabled(true)
      .setSerde("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema.toString())
      .build());

    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put(StreamConversionMapReduce.SOURCE_NAME, streamName);
    runtimeArgs.put(StreamConversionMapReduce.SINK_NAME, filesetName);

    Map<String, String> adapterProperties = Maps.newHashMap();
    adapterProperties.put(StreamConversionMapReduce.FORMAT_NAME, "string");
    adapterProperties.put(StreamConversionMapReduce.FORMAT_SETTINGS, "{}");
    adapterProperties.put(StreamConversionMapReduce.SCHEMA, schema.toString());
    adapterProperties.put(StreamConversionMapReduce.FREQUENCY, String.valueOf(600000));
    adapterProperties.put(StreamConversionMapReduce.HEADERS, "header1,header2");

    runtimeArgs.put(StreamConversionMapReduce.ADAPTER_PROPERTIES, GSON.toJson(adapterProperties));

    // run the mapreduce job and wait for it to finish
    MapReduceManager mapReduceManager = appManager.startMapReduce("StreamConversionMapReduce", runtimeArgs);
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // get the output fileset, and read the avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
    List<GenericRecord> records = Lists.newArrayList();
    for (Location dayLoc : fileSet.getUnderlyingFileSet().getBaseLocation().list()) {
      // this level should be directories of the day
      for (Location minLoc : dayLoc.list()) {
        // this level should be directories of the minute
        for (Location file : minLoc.list()) {
          // this level should be map reduce output
          String locName = file.getName();
          if (locName.endsWith(".avro")) {
            DataFileStream<GenericRecord> fileStream =
              new DataFileStream<GenericRecord>(file.getInputStream(), datumReader);
            while (fileStream.hasNext()) {
              records.add(fileStream.next());
            }
          }
        }
      }
    }

    Assert.assertEquals(3, records.size());
    Assert.assertEquals(body1, records.get(0).get("data").toString());
    Assert.assertEquals("bar", records.get(0).get("header1").toString());
    Assert.assertNull(records.get(0).get("header2"));
    Assert.assertEquals(body2, records.get(1).get("data").toString());
    Assert.assertEquals("baz", records.get(1).get("header1").toString());
    Assert.assertEquals("foo", records.get(1).get("header2").toString());
    Assert.assertEquals("more content", records.get(2).get("data").toString());
    Assert.assertNull(records.get(2).get("header1"));
    Assert.assertNull(records.get(2).get("header2"));

    appManager.stopAll();
  }
}