/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.testing.ldap;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.filterchain.IoFilterChainBuilder;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoServiceListener;
import org.apache.mina.core.service.IoServiceStatistics;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.session.IoSessionConfig;
import org.apache.mina.core.session.IoSessionDataStructureFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class UnreliableNioSocketAcceptor implements IoAcceptor {

    private final NioSocketAcceptor acceptor;

    private volatile long errorRate;

    private final Thread errorGenerator;

    public UnreliableNioSocketAcceptor(int nThreads) {
        errorGenerator = new Thread("UnreliableNioSocketAcceptor ErrorGenerator") {
            @Override
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(errorRate);
                        signalError();
                    }
                }
                catch (InterruptedException ex) {
                }
            }
        };
        errorGenerator.start();
        acceptor = new NioSocketAcceptor(nThreads);
    }

    @Override
    public SocketAddress getLocalAddress() {
        return acceptor.getLocalAddress();
    }

    @Override
    public Set<SocketAddress> getLocalAddresses() {
        return acceptor.getLocalAddresses();
    }

    @Override
    public SocketAddress getDefaultLocalAddress() {
        return acceptor.getDefaultLocalAddress();
    }

    @Override
    public List<SocketAddress> getDefaultLocalAddresses() {
        return acceptor.getDefaultLocalAddresses();
    }

    @Override
    public void setDefaultLocalAddress(SocketAddress localAddress) {
        acceptor.setDefaultLocalAddress(localAddress);
    }

    @Override
    public void setDefaultLocalAddresses(SocketAddress firstLocalAddress, SocketAddress... otherLocalAddresses) {
        acceptor.setDefaultLocalAddresses(firstLocalAddress, otherLocalAddresses);
    }

    @Override
    public void setDefaultLocalAddresses(Iterable<? extends SocketAddress> localAddresses) {
        acceptor.setDefaultLocalAddresses(localAddresses);
    }

    @Override
    public void setDefaultLocalAddresses(List<? extends SocketAddress> localAddresses) {
        acceptor.setDefaultLocalAddresses(localAddresses);
    }

    @Override
    public boolean isCloseOnDeactivation() {
        return acceptor.isCloseOnDeactivation();
    }

    @Override
    public void setCloseOnDeactivation(boolean closeOnDeactivation) {
        acceptor.setCloseOnDeactivation(closeOnDeactivation);
    }

    @Override
    public void bind() throws IOException {
        acceptor.bind();
    }

    @Override
    public void bind(SocketAddress localAddress) throws IOException {
        acceptor.bind(localAddress);
    }

    @Override
    public void bind(SocketAddress firstLocalAddress, SocketAddress... addresses) throws IOException {
        acceptor.bind(firstLocalAddress, addresses);
    }

    @Override
    public void bind(Iterable<? extends SocketAddress> localAddresses) throws IOException {
        acceptor.bind(localAddresses);
    }

    @Override
    public void unbind() {
        acceptor.unbind();
    }

    @Override
    public void unbind(SocketAddress localAddress) {
        acceptor.unbind(localAddress);
    }

    @Override
    public void unbind(SocketAddress firstLocalAddress, SocketAddress... otherLocalAddresses) {
        acceptor.unbind(firstLocalAddress, otherLocalAddresses);
    }

    @Override
    public void unbind(Iterable<? extends SocketAddress> localAddresses) {
        acceptor.unbind(localAddresses);
    }

    @Override
    public IoSession newSession(SocketAddress remoteAddress, SocketAddress localAddress) {
        return acceptor.newSession(remoteAddress, localAddress);
    }

    @Override
    public TransportMetadata getTransportMetadata() {
        return acceptor.getTransportMetadata();
    }

    @Override
    public void addListener(IoServiceListener listener) {
        acceptor.addListener(listener);
    }

    @Override
    public void removeListener(IoServiceListener listener) {
        acceptor.removeListener(listener);
    }

    @Override
    public boolean isDisposing() {
        return acceptor.isDisposing();
    }

    @Override
    public boolean isDisposed() {
        return acceptor.isDisposed();
    }

    @Override
    public void dispose() {
        acceptor.dispose();
    }

    @Override
    public IoHandler getHandler() {
        return acceptor.getHandler();
    }

    @Override
    public void setHandler(IoHandler handler) {
        acceptor.setHandler(handler);
    }

    @Override
    public Map<Long, IoSession> getManagedSessions() {
        return acceptor.getManagedSessions();
    }

    @Override
    public int getManagedSessionCount() {
        return acceptor.getManagedSessionCount();
    }

    @Override
    public IoSessionConfig getSessionConfig() {
        return acceptor.getSessionConfig();
    }

    @Override
    public IoFilterChainBuilder getFilterChainBuilder() {
        return acceptor.getFilterChainBuilder();
    }

    @Override
    public void setFilterChainBuilder(IoFilterChainBuilder builder) {
        acceptor.setFilterChainBuilder(builder);
    }

    @Override
    public DefaultIoFilterChainBuilder getFilterChain() {
        return acceptor.getFilterChain();
    }

    @Override
    public boolean isActive() {
        return acceptor.isActive();
    }

    @Override
    public long getActivationTime() {
        return acceptor.getActivationTime();
    }

    @Override
    public Set<WriteFuture> broadcast(Object message) {
        return acceptor.broadcast(message);
    }

    @Override
    public IoSessionDataStructureFactory getSessionDataStructureFactory() {
        return acceptor.getSessionDataStructureFactory();
    }

    @Override
    public void setSessionDataStructureFactory(IoSessionDataStructureFactory sessionDataStructureFactory) {
        acceptor.setSessionDataStructureFactory(sessionDataStructureFactory);
    }

    @Override
    public int getScheduledWriteBytes() {
        return acceptor.getScheduledWriteBytes();
    }

    @Override
    public int getScheduledWriteMessages() {
        return acceptor.getScheduledWriteMessages();
    }

    @Override
    public IoServiceStatistics getStatistics() {
        return acceptor.getStatistics();
    }

    public void setBacklog(int backlog) {
        acceptor.setBacklog(backlog);
    }

    public void setErrorRate(long errorRate) {
        this.errorRate = errorRate;
    }

    private void signalError() {
        try {
            for (IoSession s : getManagedSessions().values()) {
                getHandler().exceptionCaught(s, new IOException("Network cable interrupted"));
            }
        }
        catch (Exception e) {
        }
    }
}
