package cn.edu.tsinghua.iotdb.benchmark.tsdb.druid;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.finagle.Service;
import com.twitter.util.Await;
import com.twitter.util.Future;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Druid implements IDatabase {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(Druid.class);
  private String dataSource = "mypageviews";
  private Service<List<Map<String, Object>>, Integer> druidService;
  private Timestamper<Map<String, Object>> timestamper;
  private CuratorFramework curator;
  private String discoveryPath = "/druid/discovery";
  private String zookeeperIP = "localhost";
  private String zookeeperPort = "2181";
  private String indexService = "overlord";
  private String firehosePattern = "druid:firehose:%s";
  private final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto");
  private List<String> dimensions = new ArrayList<String>();
  private QueryGranularity queryGranularity = QueryGranularity.NONE; // Millisecond Querys
  private Granularity segmentGranularity = Granularity.HOUR;
  private Period windowPeriod = new Period().withDays(2);
  private int partitions = 1;
  private int replicants = 1;
  private Period warmingPeriod = new Period().withMinutes(
      10); // does not help for first task, but spawns task for second segment 10 minutes earlier, see: https://groups.google.com/forum/#!topic/druid-user/UT5JNSZqAuk
  private int retries = 3;
  private CloseableHttpClient client;
  private URL urlQuery = null;
  private String queryIP = "localhost"; // normally broker node,but historical/realtime is possible
  private String queryPort = "8090"; // normally broker node,but historical/realtime is possible
  private String queryURL = "/druid/v2/?pretty";
  private boolean useTranquility = true;

  public Druid() {
    if(useTranquility) {
      dimensions.add("device");
      for (int i = 0; i < config.SENSOR_NUMBER; i++) {
        dimensions.add("s_" + i);
      }

      timestamper = new Timestamper<Map<String, Object>>() {
        @Override
        public DateTime timestamp(Map<String, Object> theMap) {
          return new DateTime(theMap.get("timestamp"));
        }
      };
      curator = CuratorFrameworkFactory
          .builder()
          .connectString(String.format("%s:%s", zookeeperIP, zookeeperPort))
          .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
          .build();
      druidService = DruidBeams
          .builder(timestamper)
          .curator(curator)
          .discoveryPath(discoveryPath)
          .location(
              DruidLocation.create(
                  indexService,
                  firehosePattern,
                  dataSource
              )
          )
          .timestampSpec(timestampSpec)
          .rollup(DruidRollup.create(
              DruidDimensions.specific(dimensions), new ArrayList<AggregatorFactory>(),
              queryGranularity))
          .tuning(
              ClusteredBeamTuning
                  .builder()
                  .segmentGranularity(this.segmentGranularity)
                  .windowPeriod(this.windowPeriod)
                  .partitions(this.replicants)
                  .replicants(this.partitions)
                  .warmingPeriod(this.warmingPeriod)
                  .build()
          )
          .buildJavaService();
      RequestConfig requestConfig = RequestConfig.custom().build();
      client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      try {
        urlQuery = new URL("http", queryIP, Integer.valueOf(queryPort), queryURL);
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void init() throws TsdbException {

  }

  @Override
  public void cleanup() throws TsdbException {

  }

  @Override
  public void close() throws TsdbException {

  }

  @Override
  public void registerSchema(Measurement measurement) throws TsdbException {

  }

  @Override
  public Status insertOneBatch(Batch batch) {
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    String metric = dataSource;

    for (Record record : batch.getRecords()) {
      long time = record.getTimestamp();
      Map<String, Object> data = new HashMap<>();
      // Calculate special Timestamp in special druid time range
      // (workload time range shifted to actual time)
      data.put("timestamp", System.currentTimeMillis() + (time - Constants.START_TIMESTAMP));
      data.put("device", deviceSchema.getDevice());
      int i = 0;
      for (String s : deviceSchema.getSensors()) {
        data.put(s, record.getRecordDataValue().get(i));
        i++;
      }
      List<Map<String, Object>> druidEvents = new ArrayList<>();
      druidEvents.add(data);
      final Future<Integer> numSentFuture = druidService.apply(druidEvents);
      try {
        final Integer numSent = Await.result(numSentFuture);

        System.out.println(String.format("Result (numSent) from Druid: %s", numSent));


      } catch (Exception e) {
        LOGGER.error("ERROR: Error in processing insert to metric: {}", metric, e);
        return new Status(false, 0, e, e.toString());
      }
    }
    return null;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

  private JSONArray runQuery(URL url, String queryStr) {
    JSONArray jsonArr = new JSONArray();
    HttpResponse response = null;
    try {
      HttpPost postMethod = new HttpPost(url.toString());
      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
      postMethod.setEntity(requestEntity);
      postMethod.addHeader("accept", "application/json");
      int tries = retries + 1;
      while (true)
      {
        tries--;
        try
        {
          response = client.execute(postMethod);
          break;
        }
        catch (IOException e)
        {
          if (tries < 1) {
            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
            LOGGER.error("Error: ", e);
            if (response != null) {
              EntityUtils.consumeQuietly(response.getEntity());
            }
            postMethod.releaseConnection();
            return null;
          }
        }
      }
      if(response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT  ||
          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM){
        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
          System.err.println("WARNING: Query returned 301, that means 'API call has migrated or should be forwarded to another server'");
        }
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT){
          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
          BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
          StringBuilder builder = new StringBuilder();
          String line;
          while ((line = bis.readLine()) != null) {
            builder.append(line);
          }
          jsonArr = new JSONArray(builder.toString());
        }
        EntityUtils.consumeQuietly(response.getEntity());
        postMethod.releaseConnection();
      }
    } catch (Exception e) {
      System.err.println("ERROR: Error while trying to query " + url.toString() + " for '" + queryStr + "'.");
      LOGGER.error("Error: ", e);
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
      return null;
    }
    return jsonArr;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<DeviceSchema> deviceSchemas = rangeQuery.getDeviceSchema();
    JSONArray jsonArr = null;
    int line = 0;
    int queryResultPointNum = 0;
    long st;
    long en;
    try {
      JSONObject query = new JSONObject();

      query.put("queryType", "select");
      query.put("dataSource", dataSource);

      query.put("dimensions", new JSONArray());
      query.put("metrics", new JSONArray());

      JSONObject pagingSpec = new JSONObject();
      pagingSpec.put("pagingIdentifiers", new JSONObject());
      pagingSpec.put("threshold", 1);
      query.put("pagingSpec", pagingSpec);
        // WARNING: Using granularity = 1 ms for Druid is not advisable and will probably lead to problems (Search for killed Java processes due to memory).
        // See http://druid.io/docs/latest/querying/granularities.html
        // Also a Select Query needs no "none" = 1 ms granularity, see http://druid.io/docs/latest/development/select-query.html
        // it is okay to return every existing value in one big bucket, as long as all values are delivered back
//                query.put("granularity", "none");
      query.put("granularity", "all");

      JSONObject andFilter = new JSONObject();
      andFilter.put("type", "and");
      JSONArray andArray = new JSONArray();
      for (DeviceSchema deviceSchema : deviceSchemas) {
        //Map<String, String> map = new HashMap<>();
        //map.put("device", deviceSchema.getDevice());


        JSONObject orFilter = new JSONObject();
        JSONArray orArray = new JSONArray();
        orFilter.put("type", "or");
        //for (String tagValue : (ArrayList<String>) entry.getValue()) {
          JSONObject selectorFilter = new JSONObject();
          selectorFilter.put("type", "selector");
          selectorFilter.put("dimension", "device");
          selectorFilter.put("value", deviceSchema.getDevice());
          orArray.put(selectorFilter);
        //}
        orFilter.put("fields", orArray);
        andArray.put(orFilter);
      }
      andFilter.put("fields", andArray);
      query.put("filter", andFilter);

      JSONArray dateArray = new JSONArray();
      // calculate druid timestamps from workload timestamps
      dateArray.put(String.format("%s/%s",
          new DateTime(System.currentTimeMillis() + (rangeQuery.getStartTimestamp() - Constants.START_TIMESTAMP)),
          new DateTime(System.currentTimeMillis() + (rangeQuery.getEndTimestamp() - Constants.START_TIMESTAMP))));
      query.put("intervals", dateArray);
      LOGGER.info("Input Query String: {}", query.toString());

      st = System.nanoTime();
      jsonArr = runQuery(urlQuery, query.toString());
      en = System.nanoTime();
//      for (int i = 0; i < jsonArr.length(); i++) {
//        DateTime ts = new DateTime(jsonArr.getJSONObject(i).get("timestamp"));
//        JSONObject result = (JSONObject) jsonArr.getJSONObject(i).get("result");
//
//      }

      LOGGER.info("jsonArr: {}", jsonArr);
      return new Status(true, en - st, queryResultPointNum);
    } catch (Exception e) {
      LOGGER.error("ERROR: Error while processing READ for metric: ", e);
      return new Status(false, 0, queryResultPointNum, e, jsonArr.toString());
    }
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }
}
