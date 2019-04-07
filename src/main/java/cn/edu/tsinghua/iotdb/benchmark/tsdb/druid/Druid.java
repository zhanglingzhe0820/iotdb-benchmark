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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Druid implements IDatabase {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(Druid.class);
  private String dataSource = "usermetric";
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
  private Granularity segmentGranularity = Granularity.MONTH;
  private Period windowPeriod = new Period().withDays(2);
  private int partitions = 1;
  private int replicants = 1;
  private Period warmingPeriod = new Period().withMinutes(
      10); // does not help for first task, but spawns task for second segment 10 minutes earlier, see: https://groups.google.com/forum/#!topic/druid-user/UT5JNSZqAuk


  public Druid() {
    dimensions.add("device");
    dimensions.add("group");
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
      Map<String, Object> data = new HashMap<String, Object>();
      // Calculate special Timestamp in special druid time range
      // (workload time range shifted to actual time)
      data.put("timestamp", System.currentTimeMillis() + (time - Constants.START_TIMESTAMP));
      data.put("group", deviceSchema.getGroup());
      data.put("device", deviceSchema.getDevice());
      int i = 0;
      for (String s : deviceSchema.getSensors()) {
        data.put(s, record.getRecordDataValue().get(i));
        i++;
      }
      List<Map<String, Object>> druidEvents = new ArrayList<Map<String, Object>>();
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

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return null;
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
