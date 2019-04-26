package com.hny.hbase.coprocessor.client;

import com.hny.hbase.coprocessor.CountAndSumProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;
import java.util.Map;

public class CountAndSumClient {
    public static class CountAndSumResult {
        public long count;
        public double sum;

    }
    //连接
    private Connection connection;

    public CountAndSumClient(Connection connection) {
        this.connection = connection;
    }


    public CountAndSumResult call(String tableName, String family, String column, String
            startRow, String endRow) throws Throwable {
        //从服务端get的表
        Table table = connection.getTable(TableName.valueOf(tableName.getBytes()));

        //向服务端发送的request
        final com.hny.hbase.coprocessor.CountAndSumProtocol.CountAndSumRequest request = CountAndSumProtocol.CountAndSumRequest
                .newBuilder()
                .setFamily(family)
                .setColumn(column)
                .build();

        byte[] startKey = (null != startRow) ? startRow.getBytes(): null;
        byte[] endKey = (null != endRow) ? endRow.getBytes() : null;
        // coprocessorService方法的第二、三个参数是定位region的，是不是范围查询，在startKey和endKey之间的region上的数据都会参与计算
        Map<byte[], CountAndSumResult> map = table.coprocessorService(CountAndSumProtocol.RowCountAndSumService.class,
                startKey, endKey, new Batch.Call<CountAndSumProtocol.RowCountAndSumService,CountAndSumResult>() {


                    @Override
                    public CountAndSumResult call(CountAndSumProtocol.RowCountAndSumService service) throws IOException {

                        BlockingRpcCallback<CountAndSumProtocol.CountAndSumResponse> rpcCallback = new BlockingRpcCallback<CountAndSumProtocol.CountAndSumResponse>();
                        //得到response
                        service.getCountAndSum(null, request, rpcCallback);
                        CountAndSumProtocol.CountAndSumResponse response = rpcCallback.get();
                        //直接返回response也行。
                        CountAndSumResult responseInfo = new CountAndSumResult();
                        responseInfo.count = response.getCount();
                        responseInfo.sum = response.getSum();
                        return responseInfo;
                    }
                });

        CountAndSumResult result = new CountAndSumResult();

        for (CountAndSumResult ri : map.values()) {
            result.count += ri.count;
            result.sum += ri.sum;
        }



        return result;

    }

    public static void main(String[] args) throws Throwable {
        // 使用该方式需要将hbase-site.xml复制到resources目录下            *********配置
        Configuration conf = HBaseConfiguration.create();
        // hbase-site.xml不在resources目录下时使用如下方式指定
        // conf.addResource(new Path("/home/hadoop/conf/hbase", "hbase-site.xml"));
        Connection connection = ConnectionFactory.createConnection(conf);

        String tableName = "test2";
        CountAndSumClient client = new CountAndSumClient(connection);
        CountAndSumClient.CountAndSumResult result = client.call(tableName, "f1", "PBRL", null, null);

        System.out.println("count: " + result.count + ", sum: " + result.sum);
    }

}
