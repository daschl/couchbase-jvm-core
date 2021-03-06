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
package com.couchbase.client.core.endpoint.view;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.view.ViewQueryRequest;
import com.couchbase.client.core.message.view.ViewQueryResponse;
import com.couchbase.client.core.message.view.ViewRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Code which handles encoding and decoding of requests/responses against the Couchbase View Engine.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ViewCodec extends MessageToMessageCodec<HttpObject, ViewRequest> {

    /**
     * The Queue which holds the request types so that proper decoding can happen async.
     */
    private final Queue<Class<?>> queue;

    /**
     * The current request class.
     */
    private Class<?> currentRequest;

    /**
     * The current chunked up buffer.
     */
    private ByteBuf currentChunk;

    /**
     * The current state of the parser.
     */
    private ParsingState currentState = ParsingState.INITIAL;

    /**
     * The number of total rows (if parsed) for this view response.
     */
    private int currentTotalRows;

    /**
     * Creates a new {@link ViewCodec} with the default dequeue.
     */
    public ViewCodec() {
        this(new ArrayDeque<Class<?>>());
    }

    /**
     * Creates a new {@link ViewCodec} with a custom dequeue.
     *
     * @param queue a custom queue to test encoding/decoding.
     */
    public ViewCodec(final Queue<Class<?>> queue) {
        this.queue = queue;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ViewRequest msg, List<Object> out) throws Exception {
        HttpRequest request;
        if (msg instanceof ViewQueryRequest) {
            request = handleViewQueryRequest((ViewQueryRequest) msg);
        } else {
            throw new IllegalArgumentException("Unknown Messgae to encode: " + msg);
        }
        out.add(request);
        queue.offer(msg.getClass());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> in) throws Exception {
        if (currentRequest == null) {
            currentRequest = queue.poll();
            currentChunk = ctx.alloc().buffer();
        }

        if (currentRequest.equals(ViewQueryRequest.class)) {
            handleViewQueryResponse(ctx, msg, in);
        } else {
            throw new IllegalStateException("Got a response message for a request that was not sent." + msg);
        }
    }

    private HttpRequest handleViewQueryRequest(final ViewQueryRequest msg) {
        StringBuilder requestBuilder = new StringBuilder();
        requestBuilder.append("/").append(msg.bucket()).append("/_design/");
        requestBuilder.append(msg.development() ? "dev_" + msg.design() : msg.design());
        requestBuilder.append("/_view/").append(msg.view());
        if (msg.query() != null && !msg.query().isEmpty()) {
            requestBuilder.append("?").append(msg.query());
        }
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestBuilder.toString());
    }

    private void handleViewQueryResponse(ChannelHandlerContext ctx, HttpObject msg, List<Object> in) {
        switch (currentState) {
            case INITIAL:
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    // Todo: error handling or retry based on the http response code.
                    currentState = ParsingState.PREAMBLE;
                    return;
                } else {
                    throw new IllegalStateException("Only expecting HttpResponse in INITIAL");
                }
            case PREAMBLE:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }

                    int pos = currentChunk.bytesBefore((byte) ',');
                    if (pos > 0) {
                        String[] rowsInfo = currentChunk.readBytes(pos+1).toString(CharsetUtil.UTF_8).split(":");
                        currentTotalRows = Integer.parseInt(rowsInfo[1].substring(0, rowsInfo[1].length()-1));
                    } else {
                        return;
                    }
                    if (currentChunk.readableBytes() >= 8) {
                        currentChunk.readerIndex(currentChunk.readerIndex() + 8);
                    } else {
                        return;
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in PREAMBLE");
                }
                currentState = ParsingState.ROWS;
            case ROWS:
                if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    if (content.content().readableBytes() > 0) {
                        currentChunk.writeBytes(content.content());
                        content.content().clear();
                    }
                    MarkerProcessor processor = new MarkerProcessor();
                    currentChunk.forEachByte(processor);

                    boolean last = msg instanceof LastHttpContent;
                    ResponseStatus status = last ? ResponseStatus.SUCCESS : ResponseStatus.CHUNKED;
                    ByteBuf returnContent = currentChunk.readBytes(processor.marker());
                    if (processor.marker() > 0 || last) {
                        in.add(new ViewQueryResponse(status, currentTotalRows, returnContent.copy()));
                        currentChunk.discardSomeReadBytes();
                    }
                    returnContent.release();

                    if (last) {
                        currentRequest = null;
                        currentChunk.release();
                        currentChunk = null;
                        currentState = ParsingState.INITIAL;
                    }
                } else {
                    throw new IllegalStateException("Only expecting HttpContent in ROWS");
                }
        }
    }

    static enum ParsingState {
        /**
         * Start of the incremental parsing process.
         */
        INITIAL,

        /**
         * Parses non-row data like total rows and others.
         */
        PREAMBLE,

        /**
         * Parses the individual view rows.
         */
        ROWS
    }

    /**
     * A custom {@link ByteBufProcessor} which finds and counts open and closing JSON object markers.
     */
    private static class MarkerProcessor implements ByteBufProcessor {

        private int marker = 0;
        private int counter = 0;
        private int depth = 0;
        private byte open = '{';
        private byte close = '}';

        @Override
        public boolean process(byte value) throws Exception {
            counter++;
            if (value == open) {
                depth++;
            }
            if (value == close) {
                depth--;
                if (depth == 0) {
                    marker = counter;
                }
            }
            return true;
        }

        public int marker() {
            return marker;
        }
    }
}
