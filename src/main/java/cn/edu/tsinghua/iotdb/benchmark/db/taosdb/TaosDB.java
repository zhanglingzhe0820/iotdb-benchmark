package cn.edu.tsinghua.iotdb.benchmark.db.taosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaosDB implements IDatebase {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
    private static final String TAOS_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String URL_TAOS = "jdbc:TAOS://%s:%s/?user=%s&password=%s";
    private static final String USER = "root";
    private static final String PASSWD = "taosdata";
    private static final String CREATE_DATABASE = "create database if not exists %s";
    private static final String TEST_DB = "ZC";
    private static final String USE_DB = "use %s";
    private static final String CREATE_STABLE = "create table if not exists %s (ts timestamp, value float) tags(device binary(20),sensor binary(20))";
    private static final String CREATE_TABLE = "create table if not exists %s";
    private static final String INSERT_STAT = "%s%s values(%s,%s) ";
    private static final int COLUMN = 250;
    private int DEVICE_TABLE_NUM;
    private Connection connection;
    private static Config config;
    private List<Point> points;
    private Map<String, String> mp;
    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private ProbTool probTool;
    private final double unitTransfer = 1000000000.0;

    public TaosDB(long labID) throws ClassNotFoundException, SQLException {
        Class.forName(TAOS_DRIVER);
        config = ConfigDescriptor.getInstance().getConfig();
        points = new ArrayList<>();
        mp = new HashMap<>();
        mySql = new MySqlLog();
        this.labID = labID;
        sensorRandom = new Random(1 + config.QUERY_SEED);
        timestampRandom = new Random(2 + config.QUERY_SEED);
        probTool = new ProbTool();
        connection = DriverManager
                .getConnection(String.format(URL_TAOS, config.host, config.port, USER, PASSWD));
        DEVICE_TABLE_NUM = (config.SENSOR_NUMBER % COLUMN == 0) ? config.SENSOR_NUMBER / COLUMN : config.SENSOR_NUMBER / COLUMN + 1;
        mySql.initMysql(labID);
    }

    @Override
    public void init() throws SQLException {
        //delete old data of IoTDB is done in script cli-benchmark.sh
    }

    @Override
    public void createSchema() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(String.format(CREATE_DATABASE, TEST_DB));
        statement.execute(String.format(USE_DB, TEST_DB));
        for (int i = 0; i < config.DEVICE_NUMBER; i++) {
            for (int j = 0; j < DEVICE_TABLE_NUM; j++) {
                int startSensor = 0;
                int endSensor = 0;
                StringBuilder createTableSql = new StringBuilder();
                createTableSql.append(String.format(CREATE_TABLE, config.DEVICE_CODES.get(i))).append("_").append(j).append("(ts timestamp,");
                if (j == DEVICE_TABLE_NUM - 1) {
                    startSensor = j * COLUMN;
                    endSensor = config.SENSOR_NUMBER;
                } else {
                    startSensor = j * COLUMN;
                    endSensor = startSensor + COLUMN;
                }
                for (int k = startSensor; k < endSensor; k++) {
                    createTableSql.append(config.SENSOR_CODES.get(k)).append(" double,");
                }
                createTableSql.deleteCharAt(createTableSql.length() - 1).append(")");
                statement.execute(createTableSql.toString());
            }
        }
    }

    @Override
    public long getLabID() {
        return this.labID;
    }

    @Override
    public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        Statement statement;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            statement.execute(String.format(USE_DB, TEST_DB));
            if (!config.IS_OVERFLOW) {
                for (int i = 0; i < config.BATCH_SIZE; i++) {
                    createSQLStatment(loopIndex, i, device, statement);
                }
            } else {
                LOGGER.error("TAOS数据库不支持乱序插入");
                throw new RuntimeException();
            }
            long startTime = System.nanoTime();
            try {
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;
            latencies.add(costTime);
            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
            } else {
//                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
//                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
//                        (totalTime.get() + costTime) / unitTransfer,
//                        (config.BATCH_SIZE * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer,
                    totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<
            Long> totalTime,
                               ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (mySql != null) {
            mySql.closeMysql();
        }
    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        return 0;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime,
                                QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
        Statement statement = null;
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0, latency = 0;

        try {
            statement = connection.createStatement();
            statement.execute(String.format(USE_DB, TEST_DB));
            List<String> sensorList = new ArrayList<String>();

            switch (config.QUERY_CHOICE) {
                case 3:// 聚合函数查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN,
                            startTime,
                            startTime + config.QUERY_INTERVAL, sensorList);
                    break;
                case 4:// 范围查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime,
                            startTime + config.QUERY_INTERVAL, sensorList);
                    break;
                case 7:// groupBy查询
                  sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN,
                          startTime, startTime + config.QUERY_INTERVAL, sensorList);
                    break;
                case 11://值过滤的聚合查询,当前只支持查询一列
                  sql = createQuerySQLStatment(devices, config.QUERY_AGGREGATE_FUN,
                          config.QUERY_LOWER_LIMIT, sensorList);
                  break;
                default:
                    LOGGER.error("benchmark对于taos当前不支持此种查询");
            }
            int line = 0;
            LOGGER.info("{} execute {} loop,提交执行的sql：{}", Thread.currentThread().getName(), index, sql);

            startTimeStamp = System.nanoTime();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                line++;
//				int sensorNum = sensorList.size();
//				builder.append(" \ntimestamp = ").append(resultSet.getString(0)).append("; ");
//				for (int i = 1; i <= sensorNum; i++) {
//					builder.append(resultSet.getString(i)).append("; ");
//				}
            }
            statement.close();
            endTimeStamp = System.nanoTime();
            latency = endTimeStamp - startTimeStamp;
            latencies.add(latency);
//			LOGGER.info("{}",builder.toString());
            client.setTotalPoint(
                    client.getTotalPoint() + line * config.QUERY_SENSOR_NUM);
            client.setTotalTime(client.getTotalTime() + latency);

            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, (latency / 1000.0) / 1000000.0,
                    line * config.QUERY_SENSOR_NUM ,
                    line * config.QUERY_SENSOR_NUM * 1000.0 / (latency / 1000000.0),
                    (client.getTotalTime() / 1000.0) / 1000000.0, client.getTotalPoint(),
                    client.getTotalPoint() * 1000.0f / (client.getTotalTime() / 1000000.0));
            mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM ,
                    (latency / 1000.0f) / 1000000.0, config.REMARK);
        } catch (SQLException e) {
            errorCount.set(errorCount.get() + 1);
            LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(),
                    e.getMessage());
            LOGGER.error("执行失败的查询语句：{}", sql);
            mySql.saveQueryProcess(index, 0, -1, "query fail!" + sql);
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

  /**
   *值过滤的聚合查询
   */
    private String createQuerySQLStatment(List<Integer> devices, String query_aggregate_fun, double query_lower_limit, List<String> sensorList) {
      String tableName = getRandomTableName(devices);
      StringBuilder builder = new StringBuilder();
      String sqlTmp = createQuerySQLStatment(tableName, query_aggregate_fun,1);
      String sensor = sqlTmp.substring(sqlTmp.lastIndexOf("(")+1,sqlTmp.lastIndexOf(")"));
      builder.append(sqlTmp).append(" where ").append(sensor).append(" > ").append(query_lower_limit);
      return builder.toString();
    }

  /**
     * 获取device下的随即一个表
     */
    public String getRandomTableName(List<Integer> devcies) {
        int tableNum = new Random().nextInt(this.DEVICE_TABLE_NUM);
        return config.DEVICE_CODES.get(devcies.get(0)).concat("_").concat(tableNum + "");
    }

    /**
     * 范围查询
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, List<String> sensorList) {
        String tableName = getRandomTableName(devices);
        StringBuilder builder = new StringBuilder();
        builder.append(createQuerySQLStatment(tableName, num)).append(" WHERE ts >= ");
        builder.append(startTime).append(" AND ts <= ").append(endTime);
        return builder.toString();
    }

    /**
     * 创建查询语句--(查询设备下的num个传感器数值)
     */
    public String createQuerySQLStatment(String tableName, int num) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        if (num > COLUMN || num > config.SENSOR_NUMBER) {
            LOGGER.error("taos数据库不支持跨表进行查询");
        }
        int tableNum = Integer.parseInt(tableName.substring(tableName.lastIndexOf("_") + 1));
        int startSensor = 0;
        int endSensor = 0;
        if (tableNum == DEVICE_TABLE_NUM - 1) {
            startSensor = tableNum * COLUMN;
            endSensor = config.SENSOR_NUMBER;
        } else {
            startSensor = tableNum * COLUMN;
            endSensor = startSensor + COLUMN;
        }
        List<String> list = new ArrayList<String>();
        for (int i = startSensor; i < endSensor; i++) {
            list.add(config.SENSOR_CODES.get(i));
        }
        Collections.shuffle(list, sensorRandom);
        builder.append(list.get(0));
        for (int j = 1; j < num; j++) {
            builder.append(" , ").append(list.get(j));
        }
        builder.append(" FROM ").append(tableName);

        return builder.toString();
    }

    /**
     * 聚合查询
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, String query_aggregate_fun, long startTime, long endTime, List<String> sensorList) {
        String tableName = getRandomTableName(devices);
        StringBuilder builder = new StringBuilder();
        builder.append(createQuerySQLStatment(tableName, query_aggregate_fun, num)).append(" WHERE ts >= ");
        builder.append(startTime).append(" AND ts <= ").append(endTime);
        if(config.QUERY_CHOICE == 7){
          builder.append(" interval(").append(config.TIME_UNIT/1000).append("s)");
        }
        return builder.toString();
    }

    private String createQuerySQLStatment(String tableName, String query_aggregate_fun, int num) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        if (num > COLUMN || num > config.SENSOR_NUMBER) {
            LOGGER.error("taos数据库不支持跨表进行查询");
        }
        int tableNum = Integer.parseInt(tableName.substring(tableName.lastIndexOf("_") + 1));
        int startSensor = 0;
        int endSensor = 0;
        if (tableNum == DEVICE_TABLE_NUM - 1) {
            startSensor = tableNum * COLUMN;
            endSensor = config.SENSOR_NUMBER;
        } else {
            startSensor = tableNum * COLUMN;
            endSensor = startSensor + COLUMN;
        }
        List<String> list = new ArrayList<String>();
        for (int i = startSensor; i < endSensor; i++) {
            list.add(config.SENSOR_CODES.get(i));
        }
        Collections.shuffle(list, sensorRandom);
        builder.append(query_aggregate_fun).append("(").append(list.get(0)).append(")");
        for (int j = 1; j < num; j++) {
            builder.append(" , ").append(query_aggregate_fun).append("(").append(list.get(j)).append(")");
        }
        builder.append(" FROM ").append(tableName);

        return builder.toString();
    }


    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex,
                                        ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
            throws SQLException {

    }

    @Override
    public long count(String group, String device, String sensor) {
        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime,
                                      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
                                      ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex,
                                      Random random, ArrayList<Long> latencies) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<
            Long> totalTime,
                                          ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random,
                                          ArrayList<Long> latencies) throws SQLException {
        return 0;
    }

    public void createSQLStatment(int batch, int index, String device, Statement statement)
            throws SQLException {
        for (int i = 0; i < this.DEVICE_TABLE_NUM; i++) {
            int startSensor;
            int endSensor;
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("insert into ").append(device).append("_").append(i).append(" values(");
            long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.BATCH_SIZE
                    + index);
            if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
                currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
            }
            insertSql.append(currentTime).append(",");
            if (i == DEVICE_TABLE_NUM - 1) {
                startSensor = i * COLUMN;
                endSensor = config.SENSOR_NUMBER;
            } else {
                startSensor = i * COLUMN;
                endSensor = startSensor + COLUMN;
            }
            for (int j = startSensor; j < endSensor; j++) {
                /*
                FunctionParam param = config.SENSOR_FUNCTION.get(config.SENSOR_CODES.get(j));
                Number value = Function.getValueByFuntionidAndParam(param, currentTime);
                */
                Number value = currentTime;
                float v = Float.parseFloat(String.format("%.2f", value.floatValue()));
                insertSql.append(v).append(",");
            }
            insertSql.deleteCharAt(insertSql.length() - 1).append(")");
            statement.addBatch(insertSql.toString());
        }
    }
}
