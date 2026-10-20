/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.function.Supplier;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.util.Clock;

/**
 * SPNEGO (Kerberos) authentication for the low-level REST client: a keytab-based JAAS login
 * plus a request interceptor that attaches a fresh {@code Negotiate} token per request.
 * Kerberos support is a migration lifeline for Confluent V1 deployments, which have no vendor
 * migration path since V2 removed it.
 * <p>
 * The login is repeated for the life of the task. JGSS only ever reads the ticket-granting
 * ticket from the logged-in {@link Subject} and discards it once it expires, so without a new
 * login every request would fail after the ticket lifetime. The subject is therefore refreshed
 * shortly before the ticket's end time, and once more after a token failure to cover a ticket
 * invalidated ahead of that time.
 *
 * @author Chris Cranford
 * @see "DDD-61 Section 10.2"
 */
public class KerberosAuthentication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosAuthentication.class);

    private static final Oid SPNEGO_OID = spnegoOid();
    private static final String LOGIN_CONTEXT_NAME = "DebeziumElasticsearchSink";
    private static final String TGT_SERVICE_PREFIX = "krbtgt/";

    /**
     * How long before the ticket-granting ticket's end time a new login is performed, so that a
     * request never races the expiry.
     */
    static final long RELOGIN_MARGIN_MS = Duration.ofMinutes(5).toMillis();

    /**
     * The least time between a login and a failure-driven login, so that a persistent failure
     * unrelated to the ticket, such as an unregistered service principal, does not log in to
     * the KDC on every request.
     */
    static final long FAILURE_RELOGIN_COOLDOWN_MS = Duration.ofMinutes(1).toMillis();

    /**
     * Produces a SPNEGO token for the given host under the given subject.
     */
    @FunctionalInterface
    interface TokenSource {
        String token(Subject subject, String host) throws Exception;
    }

    private final Supplier<Subject> login;
    private final TokenSource tokens;
    private final Clock clock;

    private volatile Subject subject;
    private volatile long ticketEndTimeMs;
    private volatile long lastLoginMs;

    public KerberosAuthentication(String principal, String keytabPath) {
        this(() -> keytabLogin(principal, keytabPath), KerberosAuthentication::gssToken, Clock.system());
    }

    /**
     * The login yields a freshly authenticated subject and the token source performs the GSS
     * exchange; both are injectable so that the renewal policy can be exercised without a KDC.
     */
    KerberosAuthentication(Supplier<Subject> login, TokenSource tokens, Clock clock) {
        this.login = login;
        this.tokens = tokens;
        this.clock = clock;
        login();
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

    String negotiateToken(String host) {
        final Subject used = currentSubject();
        try {
            return tokens.token(used, host);
        }
        catch (Exception first) {
            final Subject retryWith = subjectForRetry(used, host, first);
            try {
                return tokens.token(retryWith, host);
            }
            catch (Exception second) {
                throw tokenFailure(host, second);
            }
        }
    }

    /**
     * A ticket can be invalidated before its end time, so a failed token attempt is retried once
     * under a fresh login. A login another request performed in the meantime is reused as is,
     * and no login follows within {@link #FAILURE_RELOGIN_COOLDOWN_MS} of the last one, in which
     * case the failure stands.
     */
    private synchronized Subject subjectForRetry(Subject used, String host, Exception failure) {
        if (subject != used) {
            return subject;
        }
        if (clock.currentTimeInMillis() - lastLoginMs < FAILURE_RELOGIN_COOLDOWN_MS) {
            throw tokenFailure(host, failure);
        }
        LOGGER.warn("Kerberos SPNEGO token for host {} could not be obtained, logging in again: {}", host, failure.getMessage());
        login();
        return subject;
    }

    private static DebeziumException tokenFailure(String host, Exception cause) {
        return new DebeziumException("Failed to obtain a Kerberos SPNEGO token for host " + host, cause);
    }

    /**
     * Returns the logged-in subject, logging in again once the ticket-granting ticket is within
     * {@link #RELOGIN_MARGIN_MS} of its end time. Concurrent requests log in once.
     */
    Subject currentSubject() {
        if (isTicketExpiring()) {
            synchronized (this) {
                if (isTicketExpiring()) {
                    LOGGER.info("Kerberos ticket-granting ticket is about to expire, logging in again");
                    login();
                }
            }
        }
        return subject;
    }

    private boolean isTicketExpiring() {
        return clock.currentTimeInMillis() >= ticketEndTimeMs - RELOGIN_MARGIN_MS;
    }

    private synchronized void login() {
        final Subject fresh = login.get();
        this.ticketEndTimeMs = ticketEndTime(fresh);
        this.lastLoginMs = clock.currentTimeInMillis();
        this.subject = fresh;
    }

    /**
     * The end time of the ticket-granting ticket in the subject, or no deadline when there is
     * none, in which case the failure-driven login is the only recovery.
     */
    private static long ticketEndTime(Subject subject) {
        long endTimeMs = Long.MAX_VALUE;
        for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
            if (ticket.getServer().getName().startsWith(TGT_SERVICE_PREFIX) && ticket.getEndTime() != null) {
                endTimeMs = Math.min(endTimeMs, ticket.getEndTime().getTime());
            }
        }
        return endTimeMs;
    }

    private static Subject keytabLogin(String principal, String keytabPath) {
        try {
            final LoginContext context = new LoginContext(LOGIN_CONTEXT_NAME, null, null,
                    keytabConfiguration(principal, keytabPath));
            context.login();
            return context.getSubject();
        }
        catch (Exception e) {
            throw new DebeziumException(String.format(
                    "Kerberos login failed for principal '%s' with keytab '%s'", principal, keytabPath), e);
        }
    }

    private static String gssToken(Subject subject, String host) throws Exception {
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
