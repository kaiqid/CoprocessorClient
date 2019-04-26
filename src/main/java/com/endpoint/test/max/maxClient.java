package com.endpoint.test.max;


import com.endpoint.test.max.maxProtocol;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class maxClient {

   //设置参数
    private static final String TABLE_NAME = "test2";
    private static final String FAMILY = "f1";
    private static final String COLUMN = "PBRL";
    private static final byte[] STRAT_KEY = Bytes.toBytes("000");
    private static final byte[] END_KEY = Bytes.toBytes("100");

    public static void main(String[] args) throws Exception {



        // 配置HBse
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux3,linux4,linux5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.setLong("hbase.rpc.timeout", 600000);
        System.setProperty("hadoop.home.dir", "F:/ruanjian/hadoop-2.6.0-cdh5.14.0");

        // 建立一个连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(TABLE_NAME));


        // 设置请求对象
        final maxProtocol.maxRequest request = maxProtocol.maxRequest.newBuilder().setFamily(FAMILY).setColumn(COLUMN).build();

        try {
            // 获得返回值
            Map<byte[], Double> result = table.coprocessorService(maxProtocol.maxService.class, STRAT_KEY,  END_KEY,
                    new Batch.Call<maxProtocol.maxService, Double>() {

                        @Override
                        public Double call(maxProtocol.maxService maxService) throws IOException {
                            BlockingRpcCallback<maxProtocol.maxResponse> rpcCallback = new BlockingRpcCallback<maxProtocol.maxResponse>();
                            maxService.getmax(null, request, rpcCallback);
                            maxProtocol.maxResponse response = rpcCallback.get();
                            return response.getMax();
                        }


                    });
            Double max = null;
            // 将返回值进行迭代相加
            for (Double temp : result.values()) {
                max = max != null && (temp == null || compare(temp, max) <= 0) ? max : temp;
            }
            // 结果输出
            System.out.println("max: " + max);

        } catch (ServiceException e) {
            e.printStackTrace();
        }catch (Throwable e) {
            e.printStackTrace();
        }
        table.close();
        conn.close();

    }

    public static int compare(Double l1, Double l2) {
        if (l1 == null ^ l2 == null) {
            return l1 == null ? -1 : 1; // either of one is null.
        } else if (l1 == null)
            return 0; // both are null
        return l1.compareTo(l2); // natural ordering.
    }

}