package com.endpoint.test.min;


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

public class minClient {

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
        final minProtocol.minRequest request = minProtocol.minRequest.newBuilder().setFamily(FAMILY).setColumn(COLUMN).build();

        try {
            // 获得返回值
            Map<byte[], Long> result = table.coprocessorService(minProtocol.minService.class, STRAT_KEY,  END_KEY,
                    new Batch.Call<minProtocol.minService, Long>() {

                        @Override
                        public Long call(minProtocol.minService minService) throws IOException {
                            BlockingRpcCallback<minProtocol.minResponse> rpcCallback = new BlockingRpcCallback<minProtocol.minResponse>();
                            minService.getmin(null, request, rpcCallback);
                            minProtocol.minResponse response = rpcCallback.get();
                            return response.getMin();
                        }


                    });
            Long min = null;
            // 将返回值进行迭代相加
            for (Long temp : result.values()) {
                min = min != null && (temp == null || compare(temp, min) >= 0) ? min : temp;
            }
            // 结果输出
            System.out.println("min: " + min);

        } catch (ServiceException e) {
            e.printStackTrace();
        }catch (Throwable e) {
            e.printStackTrace();
        }
        table.close();
        conn.close();

    }

    public static int compare(Long l1, Long l2) {
        if (l1 == null ^ l2 == null) {
            return l1 == null ? -1 : 1; // either of one is null.
        } else if (l1 == null)
            return 0; // both are null
        return l1.compareTo(l2); // natural ordering.
    }

}