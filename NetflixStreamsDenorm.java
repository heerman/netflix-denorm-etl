/**
 * Netflix Stream Data Denormalizer
 *
 * This Apache Beam pipeline prepares Netflix data (data warehousing).
 *
 * Combine
 *     Stream: message version user_id media_id timestamp length_min device_type os
 *     Media: media_id primaryTitle runtimeMinutes startYear genres titleType
 *     User: user_id country gender birth_year access
 * 
 * github.com/heerman
 *
 * Script Parameters (Required):
 *  --runner          DataflowRunner
 *  --project         <GCS Project ID>
 *  --stagingLocation <GCS Directory for Temp Files>
 *  --gcpTempLocation <GCS Directory for Temp Files>
 *  --input_streams   <Input: Netflix stream data, JSON, GZIP, gs://BUCKET/FILE.gz>
 *  --input_media     <Input: Netflix media data, JSON, GZIP, gs://BUCKET/FILE.gz>
 *  --input_users     <Input: Netflix users data, JSON, GZIP, gs://BUCKET/FILE.gz>
 *  --output          <Output: Denormalized, JSON, FLAT TXT, gs://BUCKET/FILE>
 *
 * Usage Example:
 *  mvn compile exec:java
 *    -Pdataflow-runner
 *    -Dexec.mainClass=NetflixStreamsDenorm
 *    -Dexec.args="
 *    --runner=DataflowRunner
 *    --project=my_gcp_project
 *    --input_streams=gs://mybucket_dataset/nf/streams.gz
 *    --input_tracks=gs://mybucket_dataset/nf/media.gz
 *    --input_users=gs://mybucket_dataset/nf/users.gz
 *    --stagingLocation=gs://mybucket_output/staging
 *    --gcpTempLocation=gs://mybucket_output/temp
 *    --output=gs://mybucket_output/nf/streams_denorm.json"
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.Gson;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NetflixStreamsDenorm {
  private static final Logger logging = LoggerFactory.getLogger(NetflixStreamsDenorm.class);

  /**
   * Join two PCollections containing Strings with JSON, returning one PCollection with an entry 
   * per each match.  For example, merge JSON records where artist==Prince for track and stream 
   * datasets.
   */
  public static class JoinJsonDatasets
      extends PTransform<PCollectionList<String>, PCollection<String>> {

    private String joinKey;
    private String l1;
    private String l2;

    public JoinJsonDatasets(String joinKey, String label1, String label2) {
      this.joinKey = joinKey;
      this.l1 = label1;
      this.l2 = label2;
    }


    /**
     * The "relational join" logic, basically a CoGroupByKey and a ParDo.
     */
    @Override
    public PCollection<String> expand(PCollectionList<String> pcs) {
      // Join two PCollections of JSON entries, matching values for join_key
      PCollection<String> pc1 = pcs.get(0);
      PCollection<String> pc2 = pcs.get(1);

      PCollection<KV<String, String>> pc1MappedToFieldValue = pc1
        .apply(l1 + "_from_json", ParDo.of(new MapFieldValueToJson(joinKey)));


      PCollection<KV<String, String>> pc2MappedTofieldValue = pc2
        .apply(l2 + "_from_json", ParDo.of(new MapFieldValueToJson(joinKey)));

      // Relational-Join: media, streams (cogroup)
      final TupleTag<String> tag1 = new TupleTag<>();
      final TupleTag<String> tag2 = new TupleTag<>();

      PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
        .of(tag1, pc1MappedToFieldValue)
        .and(tag2, pc2MappedTofieldValue)
        .apply(l1 + "_" + l2 + "_cogroup", CoGroupByKey.create());

      PCollection<String> pcJoined = kvpCollection
        .apply(l1 + "_" + l2 + "_combine", ParDo.of(new CombineCoGbkResults(tag1, tag2)));

      return pcJoined;

    }

  }


  /**
   * Pull a field's value from JSON, and return a map of value-to-JSON
   */
  static class MapFieldValueToJson extends DoFn<String, KV<String, String>> {
    private final String fieldKey;

    public MapFieldValueToJson(String key) {
        this.fieldKey = key;
    }

    @ProcessElement
    public void processElement(@Element String jsonText, OutputReceiver<KV<String, String>> out) {

      JsonObject jsonObject = new JsonParser().parse(jsonText).getAsJsonObject();

      if (jsonObject.has(fieldKey)) {
        String fieldValue = jsonObject.get(fieldKey).getAsString();
        out.output(KV.of(fieldValue, jsonText));
      }
      // else: only return json if a field with the given key exists
    }
  }


  /**
   * Format the output of CoGroupByKey, merging each pair of JSON strings
   */
  static class CombineCoGbkResults extends DoFn<KV<String, CoGbkResult>, String> {
    private final TupleTag<String> tag1;
    private final TupleTag<String> tag2;

    public CombineCoGbkResults(TupleTag<String> tag1, TupleTag<String> tag2) {
      this.tag1 = tag1;
      this.tag2 = tag2;
    }

    @ProcessElement
    public void processElement(@Element KV<String, CoGbkResult> gbk, OutputReceiver<String> out) {
      Gson gson = new Gson();

      // Access each element from the CoGroupByKey results using the tag variables
      Iterable<String> matchesTag1 = gbk.getValue().getAll(tag1);
      Iterable<String> matchesTag2 = gbk.getValue().getAll(tag2);

      for (String i : matchesTag1) {
        for (String j : matchesTag2) {

          JsonObject jsonObject_i = new JsonParser().parse(i).getAsJsonObject();
          JsonObject jsonObject_j = new JsonParser().parse(j).getAsJsonObject();

          Map<String, Object> attributes = new HashMap<String, Object>();

          Set<Entry<String, JsonElement>> entrySet_i = jsonObject_i.entrySet();
          for (Map.Entry<String,JsonElement> entry : entrySet_i){
            attributes.put(entry.getKey(), jsonObject_i.get(entry.getKey()));
          }

          Set<Entry<String, JsonElement>> entrySet_j = jsonObject_j.entrySet();
          for (Map.Entry<String,JsonElement> entry : entrySet_j){
            attributes.put(entry.getKey(), jsonObject_j.get(entry.getKey()));
          }

          out.output(gson.toJson(attributes));
        }
      }

    }
  }


  /**
   * Provide a list of keys and delete all but those fields from a JSON string
   */
  static class SelectJsonFields extends DoFn<String, String> {
    private final String[] keysOfInterest;

    public SelectJsonFields(String[] keysOfInterest) {
        this.keysOfInterest = keysOfInterest;
    }

    @ProcessElement
    public void processElement(@Element String jsonIn, OutputReceiver<String> out) {

      HashMap<String,String> jsonData = new HashMap<String,String>();

      JsonObject jsonObject = new JsonParser().parse(jsonIn).getAsJsonObject();
      for (String key : keysOfInterest) {

        if (jsonObject.has(key)) {
          jsonData.put(key, jsonObject.get(key).getAsString());
        } else {
          jsonData.put(key, "");
        }

      }
      Gson gson = new Gson();
      out.output(gson.toJson(jsonData));
    }
  }


  /**
   * Command-line options
   */
  public interface DenormOptions extends PipelineOptions {

    @Description("Input: Netflix stream data, JSON, GZIP, gs://BUCKET/FILE.gz")
    @Default.String("gs://my_bucket/datasets/Tech_files/streams.gz")
    String getinput_streams();

    void setinput_streams(String value);

    @Description("Input: Netflix media data, JSON, GZIP, gs://BUCKET/FILE.gz")
    @Default.String("gs://my_bucket/datasets/Tech_files/media.gz")
    String getinput_media();

    void setinput_media(String value);

    @Description("Input: Netflix users data, JSON, GZIP, gs://BUCKET/FILE.gz")
    @Default.String("gs://my_bucket/datasets/Tech_files/users.gz")
    String getinput_users();

    void setinput_users(String value);

    @Description("Output: Denormalized, JSON, FLAT TXT, gs://BUCKET/FILE.JSON")
    @Default.String("gs://my_bucket_results/output/streams_denorm.json")
    @Required
    String getoutput();

    void setoutput(String value);
  }


  /**
   * Main entry point; Join Netflix stream data with track and user data
   */
  static void runDenormalizer(DenormOptions options) {
    // Data associated with these keys is the only data we care about
    String[] STREAM_KEYS = {"message", "version", "user_id", "media_id", "timestamp", 
        "length_min", "device_type", "os"};

    String[] MEDIA_KEYS =  {"media_id", "primaryTitle", "runtimeMinutes", "startYear", "genres", 
        "titleType"};

    String[] USER_KEYS =   {"user_id", "country", "gender", "birth_year", "access"};

    // Define the dataflow pipeline
    Pipeline p = Pipeline.create(options);

    // Import streams
    PCollection<String> streams = p
      .apply("streams_import", TextIO.read()
        .from(options.getinput_streams()).withCompression(Compression.GZIP))
      .apply("streams_select_fields", ParDo.of(new SelectJsonFields(STREAM_KEYS)));

    // Import media
    PCollection<String> media = p
      .apply("media_import", TextIO.read()
        .from(options.getinput_media()).withCompression(Compression.GZIP))
      .apply("media_select_fields", ParDo.of(new SelectJsonFields(MEDIA_KEYS)));

    // Import users
    PCollection<String> users = p
      .apply("users_import", TextIO.read()
        .from(options.getinput_users()).withCompression(Compression.GZIP))
      .apply("users_select_fields", ParDo.of(new SelectJsonFields(USER_KEYS)));

    // Join media + streams
    PCollectionList<String> pcsToJoin = PCollectionList.of(media).and(streams);

    PCollection<String> stream2 = pcsToJoin
      .apply("join_media_streams", new JoinJsonDatasets("media_id", "media", "streams"));

    // Join users + (media + streams)
    PCollectionList<String> pcsToJoin2 = PCollectionList.of(users).and(stream2);

    PCollection<String> final_output = pcsToJoin2
      .apply("join_users_media_streams", new JoinJsonDatasets("user_id", "users", "stream2"));

    // Write output
    final_output.apply("WriteOutput", TextIO.write()
      .to(options.getoutput()).withCompression(Compression.GZIP));

    // Run the dataflow pipeline
    p.run().waitUntilFinish();
  }


  public static void main(String[] args) {
    DenormOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DenormOptions.class);

    runDenormalizer(options);
  }
}
