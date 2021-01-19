package com.ericguerra.batchagent.tls;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

import javax.net.ssl.SSLException;
import java.io.File;

/** Netty SSL contexts: TLS 1.3/1.2 only, client certs required on the server side. */
public final class Tls {
    private Tls() {}

    public static SslContext server(File certChain, File privateKey, File trustedCa) throws SSLException {
        return GrpcSslContexts.configure(SslContextBuilder.forServer(certChain, privateKey), SslProvider.JDK)
                .trustManager(trustedCa)
                .clientAuth(ClientAuth.REQUIRE)
                .protocols("TLSv1.3", "TLSv1.2")
                .build();
    }

    public static SslContext client(File certChain, File privateKey, File trustedCa) throws SSLException {
        return GrpcSslContexts.forClient()
                .keyManager(certChain, privateKey)
                .trustManager(trustedCa)
                .protocols("TLSv1.3", "TLSv1.2")
                .build();
    }
}
