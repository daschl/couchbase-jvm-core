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
package com.couchbase.client.core.message.cluster;

import com.couchbase.client.core.cluster.Cluster;
import com.couchbase.client.core.message.AbstractCouchbaseRequest;

import java.util.Arrays;
import java.util.List;

/**
 * Sets the nodes to bootstrap from in the {@link Cluster}.
 *
 * For stability reasons, it is advised to always provide more than one seed node (but not necessarily all nodes from
 * the cluster) so that the cluster can correctly bootstrap the bucket, even if one of the hosts in the list is
 * currently not available.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class SeedNodesRequest extends AbstractCouchbaseRequest implements ClusterRequest {

    /**
     * The default hostname which will be used if the default constructor is used.
     */
    private static final String DEFAULT_HOSTNAME = "localhost";

    /**
     * The list of hostnames/IPs.
     */
    private List<String> nodes;

    /**
     * Creates a {@link SeedNodesRequest} with the default hostname ("localhost").
     */
    public SeedNodesRequest() {
        this(DEFAULT_HOSTNAME);
    }

    /**
     * Creates a {@link SeedNodesRequest} with the given hostnames.
     *
     * @param nodes the seed node hostnames.
     */
    public SeedNodesRequest(final String... nodes) {
        this(Arrays.asList(nodes));
    }

    /**
     * Creates a {@link SeedNodesRequest} with the given list of hostnames.
     *
     * @param nodes the seed node hostnames.
     */
    public SeedNodesRequest(final List<String> nodes) {
        super(null, null);
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("At least one hostname needs to be provided.");
        }
        this.nodes = nodes;
    }

    /**
     * Returns the set list of seed hostnames.
     *
     * @return the list of hostnames.
     */
    public List<String> nodes() {
        return nodes;
    }
}
