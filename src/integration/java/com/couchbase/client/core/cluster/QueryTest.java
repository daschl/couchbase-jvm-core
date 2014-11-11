package com.couchbase.client.core.cluster;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.util.TestProperties;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

public class QueryTest {

    @Test
    public void shouldRunQuery() throws Exception {
        final CouchbaseCore cluster = new CouchbaseCore();
        cluster.<SeedNodesResponse>send(new SeedNodesRequest("192.168.56.101")).flatMap(
            new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                @Override
                public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                    return cluster.send(new OpenBucketRequest(TestProperties.bucket(), TestProperties.password()));
                }
            }
        ).toBlocking().single();


        while(true) {
            GenericQueryResponse res = cluster.<GenericQueryResponse>send(new GenericQueryRequest("select name from `beer-sample` limit 3;", null, null)).toBlocking().single();
            System.err.println(res);
            res.rows().toBlocking().forEach(new Action1<ByteBuf>() {
                @Override
                public void call(ByteBuf byteBuf) {
                    System.err.println(byteBuf.toString(CharsetUtil.UTF_8));
                }
            });

            Thread.sleep(1000);
        }

    }

}
