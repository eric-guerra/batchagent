package com.ericguerra.batchagent.grpc;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ClientCertAuthInterceptorTest {
    final ClientCertAuthInterceptor acl = new ClientCertAuthInterceptor(Map.of(
            "dagrunner-scheduler", Set.of("*"),
            "monitoring", Set.of("ListJobs")));

    @Test
    void parsesCnFromDistinguishedName() {
        assertThat(ClientCertAuthInterceptor.cnFromDn("CN=dagrunner-scheduler,OU=Platform,O=Retail Co,C=US")).contains("dagrunner-scheduler");
        assertThat(ClientCertAuthInterceptor.cnFromDn("OU=NoCn,O=Retail")).isEqualTo(Optional.empty());
        assertThat(ClientCertAuthInterceptor.cnFromDn("not a dn")).isEqualTo(Optional.empty());
    }

    @Test
    void appliesPerMethodAcl() {
        assertThat(acl.isAllowed("dagrunner-scheduler", "RunJob")).isTrue();
        assertThat(acl.isAllowed("monitoring", "ListJobs")).isTrue();
        assertThat(acl.isAllowed("monitoring", "RunJob")).isFalse();
        assertThat(acl.isAllowed("someone-else", "ListJobs")).isFalse();
    }
}
