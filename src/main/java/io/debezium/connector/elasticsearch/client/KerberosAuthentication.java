/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.protocol.HttpCoreContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import io.debezium.DebeziumException;

/**
 * SPNEGO (Kerberos) authentication for the low-level REST client: a keytab-based JAAS login
 * plus a request interceptor that attaches a fresh {@code Negotiate} token per request.
 * Kerberos support is a migration lifeline for Confluent V1 deployments, which have no vendor
 * migration path since V2 removed it.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 10.2"
 */
public class KerberosAuthentication {

    private static final Oid SPNEGO_OID = spnegoOid();

    private final Subject subject;

    public KerberosAuthentication(String principal, String keytabPath) {
        try {
            final LoginContext login = new LoginContext("DebeziumElasticsearchSink", null, null,
                    keytabConfiguration(principal, keytabPath));
            login.login();
            this.subject = login.getSubject();
        }
        catch (Exception e) {
            throw new DebeziumException(String.format(
                    "Kerberos login failed for principal '%s' with keytab '%s'", principal, keytabPath), e);
        }
    }

    /**
     * Installs the SPNEGO interceptor on the async HTTP client.
     */
    public HttpAsyncClientBuilder apply(HttpAsyncClientBuilder builder) {
        return builder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
            final HttpHost target = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
            request.setHeader("Authorization", "Negotiate " + negotiateToken(target.getHostName()));
        });
    }

    private String negotiateToken(String host) {
        try {
            return Subject.doAs(subject, (PrivilegedExceptionAction<String>) () -> {
                final GSSManager manager = GSSManager.getInstance();
                final GSSName serverName = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
                final GSSContext context = manager.createContext(serverName, SPNEGO_OID, null, GSSContext.DEFAULT_LIFETIME);
                try {
                    context.requestMutualAuth(false);
                    return Base64.getEncoder().encodeToString(context.initSecContext(new byte[0], 0, 0));
                }
                finally {
                    context.dispose();
                }
            });
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to obtain a Kerberos SPNEGO token for host " + host, e);
        }
    }

    private static Configuration keytabConfiguration(String principal, String keytabPath) {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                final Map<String, Object> options = Map.of(
                        "useKeyTab", "true",
                        "keyTab", keytabPath,
                        "principal", principal,
                        "storeKey", "true",
                        "doNotPrompt", "true",
                        "isInitiator", "true",
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
