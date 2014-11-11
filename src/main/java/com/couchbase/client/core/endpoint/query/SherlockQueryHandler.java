/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.endpoint.query;

import com.couchbase.client.core.ResponseEvent;
import com.couchbase.client.core.endpoint.AbstractEndpoint;
import com.couchbase.client.core.endpoint.AbstractGenericHandler;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.query.GenericQueryRequest;
import com.couchbase.client.core.message.query.GenericQueryResponse;
import com.couchbase.client.core.message.query.QueryRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.RingBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.subjects.ReplaySubject;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Just because everyone loves temporary stuff that makes it into production code eventually.
 *
 * @author Michael Nitschinger
 * @since 2.1
 */
public class SherlockQueryHandler extends AbstractGenericHandler<HttpObject, HttpRequest, QueryRequest> {

    private static ObjectMapper JACKSON = new ObjectMapper();

    private ByteBuf currentChunks;
    private ResponseStatus currentStatus;
    private ReplaySubject<ByteBuf> currentRows;

    /**
     * Creates a new {@link SherlockQueryHandler} with the default queue for requests.
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     */
    public SherlockQueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer) {
        super(endpoint, responseBuffer);
    }

    /**
     * Creates a new {@link SherlockQueryHandler} with a custom queue for requests (suitable for tests).
     *
     * @param endpoint the {@link AbstractEndpoint} to coordinate with.
     * @param responseBuffer the {@link RingBuffer} to push responses into.
     * @param queue the queue which holds all outstanding open requests.
     */
    SherlockQueryHandler(AbstractEndpoint endpoint, RingBuffer<ResponseEvent> responseBuffer, Queue<QueryRequest> queue) {
        super(endpoint, responseBuffer, queue);
    }

    @Override
    protected HttpRequest encodeRequest(final ChannelHandlerContext ctx, final QueryRequest msg) throws Exception {
        FullHttpRequest request;

        if (msg instanceof GenericQueryRequest) {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/query");
            request.headers().set(HttpHeaders.Names.USER_AGENT, env().userAgent());
            ByteBuf query = ctx.alloc().buffer(((GenericQueryRequest) msg).query().length());
            query.writeBytes(((GenericQueryRequest) msg).query().getBytes(CHARSET));
            request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, query.readableBytes());
            request.content().writeBytes(query);
            query.release();
        } else {
            throw new IllegalArgumentException("Unknown incoming QueryRequest type "
                + msg.getClass());
        }

        return request;
    }

    @Override
    protected CouchbaseResponse decodeResponse(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        QueryRequest request = currentRequest();
        CouchbaseResponse response = null;

        if (msg instanceof HttpResponse) {
            currentChunks = ctx.alloc().buffer();
            currentStatus = statusFromCode(((HttpResponse) msg).getStatus().code());
        }

        if (msg instanceof HttpContent) {
            currentChunks.writeBytes(((HttpContent) msg).content());

            if (msg instanceof LastHttpContent) {
                // DONT TRY THIS AT HOME
                currentRows = ReplaySubject.create();
                String json = currentChunks.toString(CharsetUtil.UTF_8);
                currentChunks.release();
                Map decoded =  JACKSON.readValue(json, Map.class);
                List results = (List) decoded.get("results");
                for (Object result : results) {
                    String rowRaw = JACKSON.writeValueAsString(result);
                    ByteBuf row = ctx.alloc().buffer(rowRaw.length());
                    row.writeBytes(rowRaw.getBytes(CharsetUtil.UTF_8));
                    currentRows.onNext(row);
                }
                currentRows.onCompleted();
                response = new GenericQueryResponse(currentRows, Observable.<ByteBuf>empty(), currentStatus, request);
                finishedDecoding();
            }
        }

        return response;
    }

    /**
     * Converts a HTTP status code in its appropriate {@link ResponseStatus} representation.
     *
     * @param code the http code.
     * @return the parsed status.
     */
    private static ResponseStatus statusFromCode(int code) {
        ResponseStatus status;
        switch(code) {
            case 200:
            case 201:
            case 202:
                status = ResponseStatus.SUCCESS;
                break;
            case 404:
                status = ResponseStatus.NOT_EXISTS;
                break;
            default:
                status = ResponseStatus.FAILURE;
        }
        return status;
    }
}
