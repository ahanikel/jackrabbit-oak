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

import java.net.InetSocketAddress;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.mina.core.service.IoAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnreliableTcpTransport extends TcpTransport {
    /** A logger for this class */
    private static final Logger LOG = LoggerFactory.getLogger( UnreliableTcpTransport.class );

    private long errorRate = 0;

    /**
     * Creates an instance of the UnreliableTCPTransport class 
     */
    public UnreliableTcpTransport()
    {
        super();
    }
    
    
    /**
     * Creates an instance of the UnreliableTCPTransport class on localhost
     * @param tcpPort The port
     */
    public UnreliableTcpTransport( int tcpPort )
    {
        super( null, tcpPort, DEFAULT_NB_THREADS, DEFAULT_BACKLOG_NB );
        
        this.acceptor = createAcceptor( null, tcpPort, DEFAULT_NB_THREADS, DEFAULT_BACKLOG_NB );
        
        LOG.debug( "TCP Transport created : <*:{},>", tcpPort );
    }
    
    
    /**
     * Creates an instance of the UnreliableTCPTransport class on localhost
     * @param tcpPort The port
     * @param nbThreads The number of threads to create in the acceptor
     */
    public UnreliableTcpTransport( int tcpPort, int nbThreads )
    {
        super( null, tcpPort, nbThreads, DEFAULT_BACKLOG_NB );
        
        this.acceptor = createAcceptor( null, tcpPort, nbThreads, DEFAULT_BACKLOG_NB );
        
        LOG.debug( "TCP Transport created : <*:{},>", tcpPort );
    }
    
    
    /**
     * Creates an instance of the UnreliableTCPTransport class 
     * @param address The address
     * @param tcpPort The port
     */
    public UnreliableTcpTransport( String address, int tcpPort )
    {
        super( address, tcpPort, DEFAULT_NB_THREADS, DEFAULT_BACKLOG_NB );
        this.acceptor = createAcceptor( address, tcpPort, DEFAULT_NB_THREADS, DEFAULT_BACKLOG_NB );

        LOG.debug( "TCP Transport created : <{}:{}>", address, tcpPort );
    }
    
    
    /**
     * Creates an instance of the UnreliableTCPTransport class on localhost
     * @param tcpPort The port
     * @param nbThreads The number of threads to create in the acceptor
     * @param backLog The queue size for incoming messages, waiting for the
     * acceptor to be ready
     */
    public UnreliableTcpTransport( int tcpPort, int nbThreads, int backLog )
    {
        super( LOCAL_HOST, tcpPort, nbThreads, backLog );
        this.acceptor = createAcceptor( null, tcpPort, nbThreads, backLog );

        LOG.debug( "TCP Transport created : <*:{},>", tcpPort );
    }
    
    
    /**
     * Creates an instance of the UnreliableTCPTransport class 
     * @param address The address
     * @param tcpPort The port
     * @param nbThreads The number of threads to create in the acceptor
     * @param backLog The queue size for incoming messages, waiting for the
     * acceptor to be ready
     */
    public UnreliableTcpTransport( String address, int tcpPort, int nbThreads, int backLog )
    {
        super( address, tcpPort, nbThreads, backLog );
        this.acceptor = createAcceptor( address, tcpPort, nbThreads, backLog );

        LOG.debug( "TCP Transport created : <{}:{},>", address, tcpPort );
    }

    /**
     * Creates an instance of the UnreliableTCPTransport class on localhost
     * @param tcpPort The port
     * @param errorRate The number of milliseconds until the next error occurs
     */
    public UnreliableTcpTransport( int tcpPort, long errorRate )
    {
        super( tcpPort );
        this.errorRate = errorRate;
    }
    
    
    /**
     * Initialize the Acceptor if needed
     */
    @Override
    public void init()
    {
        acceptor = createAcceptor( getAddress(), getPort(), getNbThreads(), getBackLog() );
    }
    
    
    /**
     * Helper method to create an IoAcceptor
     */
    private IoAcceptor createAcceptor( String address, int port, int nbThreads, int backLog )
    {
        UnreliableNioSocketAcceptor acc = new UnreliableNioSocketAcceptor( nbThreads );
        acc.setBacklog( backLog );
        acc.setErrorRate( errorRate );
        
        InetSocketAddress socketAddress;
        
        // The address can be null here, if one want to connect using the wildcard address
        if ( address == null )
        {
            // Create a socket listening on the wildcard address
            socketAddress = new InetSocketAddress( port );
        }
        else
        {
            socketAddress = new InetSocketAddress( address, port );
        }
        
        acc.setDefaultLocalAddress( socketAddress );
        
        return acc;
    }
    
    
    /**
     * @return a string representation of this object
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        return "UnreliableTcpTransport" + super.toString();
    }
}
