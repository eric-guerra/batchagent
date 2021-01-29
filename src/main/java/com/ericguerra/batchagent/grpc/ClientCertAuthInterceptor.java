package com.ericguerra.batchagent.grpc;

import io.grpc.*;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * mTLS already proves the peer holds a cert signed by our CA; this decides *which* identities may
 * call which methods. The scheduler can run jobs; the monitoring service may only list them.
 */
public class ClientCertAuthInterceptor implements ServerInterceptor {
    public static final Context.Key<String> CALLER = Context.key("caller-cn");

    private final Map<String, Set<String>> allowedMethodsByCn;

    public ClientCertAuthInterceptor(Map<String, Set<String>> allowedMethodsByCn) {
        this.allowedMethodsByCn = allowedMethodsByCn;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
        SSLSession ssl = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        Optional<String> cn = ssl == null ? Optional.empty() : peerCn(ssl);
        String method = call.getMethodDescriptor().getBareMethodName();
        if (cn.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("client certificate required"), new Metadata());
            return new ServerCall.Listener<Q>() {};
        }
        if (!isAllowed(cn.get(), method)) {
            call.close(Status.PERMISSION_DENIED.withDescription(cn.get() + " may not call " + method), new Metadata());
            return new ServerCall.Listener<Q>() {};
        }
        return Contexts.interceptCall(Context.current().withValue(CALLER, cn.get()), call, headers, next);
    }

    boolean isAllowed(String cn, String method) {
        Set<String> methods = allowedMethodsByCn.get(cn);
        return methods != null && (methods.contains("*") || methods.contains(method));
    }

    static Optional<String> peerCn(SSLSession ssl) {
        try {
            Certificate[] chain = ssl.getPeerCertificates();
            if (chain.length == 0 || !(chain[0] instanceof X509Certificate)) return Optional.empty();
            return cnFromDn(((X509Certificate) chain[0]).getSubjectX500Principal().getName());
        } catch (SSLPeerUnverifiedException e) {
            return Optional.empty();
        }
    }

    static Optional<String> cnFromDn(String dn) {
        try {
            for (Rdn rdn : new LdapName(dn).getRdns()) {
                if (rdn.getType().equalsIgnoreCase("CN")) return Optional.of(rdn.getValue().toString());
            }
        } catch (InvalidNameException ignored) {
            // fall through
        }
        return Optional.empty();
    }
}
