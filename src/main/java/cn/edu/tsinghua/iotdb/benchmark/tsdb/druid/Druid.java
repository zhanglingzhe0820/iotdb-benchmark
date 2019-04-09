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
import com.alibaba.fastjson.JSON;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Druid implements IDatabase {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(Druid.class);
  private CloseableHttpClient client;
  private Properties props = new Properties();
  private String Url = config.DB_URL;
  private String writeUrl = Url + "/v1/post/wikipedia";
  private String queryUrl = Url + "/druid/v2?pretty";
  private int recordNum = 0;
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  Producer<String, String> producer ;

  public Druid() {
    RequestConfig requestConfig = RequestConfig.custom().build();
    client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<>(props);
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

  private static String getInsertJsonString(int i) {
    String[] data = {
        "{\"time\":\"2015-09-12T00:46:58.771Z\",\"channel\":\"#en.wikipedia\",\"cityName\":null,\"comment\":\"added project\",\"countryIsoCode\":null,\"countryName\":null,\"isAnonymous\":false,\"isMinor\":false,\"isNew\":false,\"isRobot\":false,\"isUnpatrolled\":false,\"metroCode\":null,\"namespace\":\"Talk\",\"page\":\"Talk:Oswald Tilghman\",\"regionIsoCode\":null,\"regionName\":null,\"user\":\"GELongstreet\",\"delta\":36,\"added\":36,\"deleted\":0}",
        "{\"time\":\"2015-09-12T00:47:00.496Z\",\"channel\":\"#ca.wikipedia\",\"cityName\":null,\"comment\":\"Robot inserta {{Commonscat}} que enllaça amb [[commons:category:Rallicula]]\",\"countryIsoCode\":null,\"countryName\":null,\"isAnonymous\":false,\"isMinor\":true,\"isNew\":false,\"isRobot\":true,\"isUnpatrolled\":false,\"metroCode\":null,\"namespace\":\"Main\",\"page\":\"Rallicula\",\"regionIsoCode\":null,\"regionName\":null,\"user\":\"PereBot\",\"delta\":17,\"added\":17,\"deleted\":0}",
        "{\"time\":\"2015-09-12T00:47:05.474Z\",\"channel\":\"#en.wikipedia\",\"cityName\":\"Auburn\",\"comment\":\"/* Status of peremptory norms under international law */ fixed spelling of 'Wimbledon'\",\"countryIsoCode\":\"AU\",\"countryName\":\"Australia\",\"isAnonymous\":true,\"isMinor\":false,\"isNew\":false,\"isRobot\":false,\"isUnpatrolled\":false,\"metroCode\":null,\"namespace\":\"Main\",\"page\":\"Peremptory norm\",\"regionIsoCode\":\"NSW\",\"regionName\":\"New South Wales\",\"user\":\"60.225.66.142\",\"delta\":0,\"added\":0,\"deleted\":0}",
        "{\"time\":\"2015-09-12T00:47:08.770Z\",\"channel\":\"#vi.wikipedia\",\"cityName\":null,\"comment\":\"fix Lỗi CS1: ngày tháng\",\"countryIsoCode\":null,\"countryName\":null,\"isAnonymous\":false,\"isMinor\":true,\"isNew\":false,\"isRobot\":true,\"isUnpatrolled\":false,\"metroCode\":null,\"namespace\":\"Main\",\"page\":\"Apamea abruzzorum\",\"regionIsoCode\":null,\"regionName\":null,\"user\":\"Cheers!-bot\",\"delta\":18,\"added\":18,\"deleted\":0}"
    };
    return data[i % data.length];
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    //int i = 0;
    //String response;
    //String body = "";

//    StringBuilder body = new StringBuilder();
//
//    for (Record record : batch.getRecords()) {
//      String data = getInsertJsonString(i);
//      long time = record.getTimestamp();
//      Map map = (Map) JSON.parse(data);
//      map.put("time", time);
//      String s = JSON.toJSONString(map);
//      i++;
//      body.append(s);
//
//    }
//    LOGGER.info("body = {}", body);
//    long st = System.nanoTime();
//    HttpResponse response = null;
//    HttpPost postMethod = new HttpPost(writeUrl);
//    StringEntity requestEntity = new StringEntity(body.toString(), ContentType.APPLICATION_JSON);
//    postMethod.setEntity(requestEntity);
//    postMethod.addHeader("accept", "application/json");
//    try {
//      response = client.execute(postMethod);
//    } catch (IOException e) {
//      LOGGER.error("insert fail because ", e);
//      return new Status(false, 0, e, e.toString());
//    } finally {
//      postMethod.releaseConnection();
//    }
//    LOGGER.info("response = {}", response);
//    long en = System.nanoTime();
//    return new Status(true, en - st);

    long st = System.nanoTime();
    for (Record record : batch.getRecords()) {
      recordNum++;
      String data = getInsertJsonString(recordNum);
      String timeString = sdf.format(new Date(record.getTimestamp()));
      Map map = (Map) JSON.parse(data);
      map.put("time", timeString);
      String s = JSON.toJSONString(map);
      LOGGER.info("record: {}", s);
      try {
        producer.send(
            new ProducerRecord<String, String>("wikipedia", Integer.toString(recordNum), s));
      } catch (Exception e) {
        LOGGER.error("insertion fail because ", e);
        return new Status(false, 0, e, e.toString());
      }
    }
    long en = System.nanoTime();
    return new Status(true, en - st);

  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

//  private JSONArray runQuery(URL url, String queryStr) {
//    JSONArray jsonArr = new JSONArray();
//    HttpResponse response = null;
//    try {
//
//      HttpPost postMethod = new HttpPost(url.toString());
//
//      StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
//      postMethod.setEntity(requestEntity);
//      postMethod.addHeader("accept", "application/json");
//      int tries = retries + 1;
//      while (true)
//      {
//        tries--;
//        try
//        {
//          response = client.execute(postMethod);
//          break;
//        }
//        catch (IOException e)
//        {
//          if (tries < 1) {
//            System.err.print("ERROR: Connection to " + url.toString() + " failed " + retries + "times.");
//            LOGGER.error("Error: ", e);
//            if (response != null) {
//              EntityUtils.consumeQuietly(response.getEntity());
//            }
//            postMethod.releaseConnection();
//            return null;
//          }
//        }
//      }
//      if(response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK ||
//          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_NO_CONTENT  ||
//          response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM){
//        if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_MOVED_PERM) {
//          System.err.println("WARNING: Query returned 301, that means 'API call has migrated or should be forwarded to another server'");
//        }
//        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT){
//          // Maybe also not HTTP_MOVED_PERM? Can't Test it right now
//          BufferedReader bis = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
//          StringBuilder builder = new StringBuilder();
//          String line;
//          while ((line = bis.readLine()) != null) {
//            builder.append(line);
//          }
//          jsonArr = new JSONArray(builder.toString());
//        }
//        EntityUtils.consumeQuietly(response.getEntity());
//        postMethod.releaseConnection();
//      }
//    } catch (Exception e) {
//      System.err.println("ERROR: Error while trying to query " + url.toString() + " for '" + queryStr + "'.");
//      LOGGER.error("Error: ", e);
//      if (response != null) {
//        EntityUtils.consumeQuietly(response.getEntity());
//      }
//      return null;
//    }
//    return jsonArr;
//  }

  /**
   * {
   *    "queryType": "scan",
   *    "dataSource": "wikipedia",
   *    "resultFormat": "list",
   *    "columns":[],
   *    "intervals": [
   *      "2013-01-01/2013-01-02"
   *    ],
   *    "batchSize":20480,
   *    "limit":5
   *  }
   *
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    long st;
    long en;
    int queryResultPointNum = 0;
    long start = rangeQuery.getStartTimestamp();
    long end = rangeQuery.getEndTimestamp();
    Map<String, Object> queryMap = new HashMap<>();
    queryMap.put("queryType", "scan");
    queryMap.put("dataSource", "wikipedia");
    queryMap.put("resultFormat", "list");
    String interval;
    if(config.QUERY_CHOICE != 0) {
      interval = sdf.format(new Date(start)) + "/" + sdf.format(new Date(end));
    } else {
      long startDelta = start - Constants.START_TIMESTAMP;
      long endDelta = end - Constants.START_TIMESTAMP;
      long lastTime = Constants.START_TIMESTAMP + config.LOOP * config.BATCH_SIZE * config.POINT_STEP;
      interval = sdf.format(new Date(lastTime - endDelta)) + "/" + sdf.format(new Date(lastTime - startDelta));
    }
    queryMap.put("intervals", interval);
    HttpPost postMethod = new HttpPost(queryUrl);
    String queryStr = JSON.toJSONString(queryMap);
    StringEntity requestEntity = new StringEntity(queryStr, ContentType.APPLICATION_JSON);
    postMethod.setEntity(requestEntity);
    postMethod.addHeader("accept", "application/json");
    HttpResponse response = null;
    try {
      st = System.nanoTime();
      response = client.execute(postMethod);
      en = System.nanoTime();
      LOGGER.info("response: {}", response);
      if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_NO_CONTENT) {
        BufferedReader bis = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent()));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = bis.readLine()) != null) {
          builder.append(line);
        }
        JSONArray jsonArray = new JSONArray(builder.toString());
        queryResultPointNum = jsonArray.length();
        LOGGER.info("jsonArray = {}", jsonArray);
      }
      EntityUtils.consumeQuietly(response.getEntity());
      postMethod.releaseConnection();

      return new Status(true, en - st, queryResultPointNum);
    } catch (IOException e) {
      LOGGER.info("response: {}", response);
      LOGGER.error("query fail because ", e);
      return new Status(false, 0, queryResultPointNum, e, "error");
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

  public static void main(String[] args) {
    String data = getInsertJsonString(0);
    long time = 1335;
    Map map = (Map) JSON.parse(data);
    map.put("time", time);
    String s = JSON.toJSONString(map);
    System.out.println(s);
    System.out.println(map);
  }

}


