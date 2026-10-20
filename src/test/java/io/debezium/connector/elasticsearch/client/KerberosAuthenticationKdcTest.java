/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Comparator;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.http.HttpHost;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.NetworkUtil;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.elasticsearch.util.StubElasticsearch;

/**
 * Exercises the real keytab login and SPNEGO exchange against an in-process KDC: the token the
 * connector attaches is accepted under the service principal the KDC issued it for.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class KerberosAuthenticationKdcTest {

    private static final String REALM = "EXAMPLE.COM";
    private static final String CLIENT = "debezium";
    private static final Oid SPNEGO_OID = spnegoOid();

    private static SimpleKdcServer kdc;
    private static File workDir;
    private static File clientKeytab;
    private static File serviceKeytab;
    private static String serviceHost;
    private static String previousKrb5Conf;

    @BeforeAll
    static void startKdc() throws Exception {
        previousKrb5Conf = System.getProperty("java.security.krb5.conf");
        workDir = Files.createTempDirectory("debezium-kdc").toFile();
        // JGSS canonicalizes host-based service names, so the service principal is registered
        // under the name the resolver will produce for the loopback host.
        serviceHost = InetAddress.getByName("localhost").getCanonicalHostName();

        kdc = new SimpleKdcServer();
        kdc.setKdcRealm(REALM);
        kdc.setKdcHost("localhost");
        kdc.setAllowUdp(false);
        kdc.setAllowTcp(true);
        kdc.setKdcTcpPort(NetworkUtil.getServerPort());
        kdc.setWorkDir(workDir);
        kdc.init();
        kdc.start();

        clientKeytab = new File(workDir, "client.keytab");
        serviceKeytab = new File(workDir, "service.keytab");
        kdc.createAndExportPrincipals(clientKeytab, CLIENT);
        kdc.createAndExportPrincipals(serviceKeytab, "HTTP/" + serviceHost);
    }

    @AfterAll
    static void stopKdc() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
        if (previousKrb5Conf == null) {
            System.clearProperty("java.security.krb5.conf");
        }
        else {
            System.setProperty("java.security.krb5.conf", previousKrb5Conf);
        }
        if (workDir != null) {
            try (var files = Files.walk(workDir.toPath())) {
                files.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    @Test
    void shouldObtainATokenTheServiceAccepts() throws Exception {
        final KerberosAuthentication auth = new KerberosAuthentication(CLIENT + "@" + REALM, clientKeytab.getAbsolutePath());

        final String token = auth.negotiateToken(serviceHost);

        assertThat(acceptedInitiator(token)).startsWith(CLIENT + "@" + REALM);
    }

    @Test
    void shouldAttachTheNegotiateHeaderToARequest() throws Exception {
        final KerberosAuthentication auth = new KerberosAuthentication(CLIENT + "@" + REALM, clientKeytab.getAbsolutePath());

        try (StubElasticsearch stub = new StubElasticsearch();
                RestClient client = RestClient.builder(new HttpHost(serviceHost, HttpHost.create(stub.url()).getPort(), "http"))
                        .setHttpClientConfigCallback(auth::apply).build()) {
            assertThat(client.performRequest(new Request("GET", "/")).getStatusLine().getStatusCode()).isEqualTo(200);

            final String header = stub.requests().get(0).headers().get("authorization").get(0);
            assertThat(header).startsWith("Negotiate ");
            assertThat(acceptedInitiator(header.substring("Negotiate ".length()))).startsWith(CLIENT + "@" + REALM);
        }
    }

    @Test
    void shouldFailTheLoginForAnUnknownPrincipal() {
        assertThatThrownBy(() -> new KerberosAuthentication("nobody@" + REALM, clientKeytab.getAbsolutePath()))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("nobody@" + REALM)
                .hasMessageContaining(clientKeytab.getAbsolutePath());
    }

    /**
     * Accepts the token under the service principal and returns the initiator it authenticates.
     */
    private static String acceptedInitiator(String token) throws Exception {
        final LoginContext login = new LoginContext("service", null, null, serviceConfiguration());
        login.login();
        try {
            return Subject.doAs(login.getSubject(), (PrivilegedExceptionAction<String>) () -> {
                final GSSManager manager = GSSManager.getInstance();
                final GSSCredential credential = manager.createCredential(null, GSSCredential.INDEFINITE_LIFETIME, SPNEGO_OID,
                        GSSCredential.ACCEPT_ONLY);
                final GSSContext context = manager.createContext(credential);
                try {
                    final byte[] bytes = Base64.getDecoder().decode(token);
                    context.acceptSecContext(bytes, 0, bytes.length);
                    return context.getSrcName().toString();
                }
                finally {
                    context.dispose();
                    credential.dispose();
                }
            });
        }
        finally {
            login.logout();
        }
    }

    private static Configuration serviceConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                final Map<String, Object> options = Map.of(
                        "useKeyTab", "true",
                        "keyTab", serviceKeytab.getAbsolutePath(),
                        "principal", "HTTP/" + serviceHost + "@" + REALM,
                        "storeKey", "true",
                        "doNotPrompt", "true",
                        "isInitiator", "false",
                        "refreshKrb5Config", "true");
                return new AppConfigurationEntry[]{ new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
            }
        };
    }

    private static Oid spnegoOid() {
        try {
            return new Oid("1.3.6.1.5.5.2");
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
