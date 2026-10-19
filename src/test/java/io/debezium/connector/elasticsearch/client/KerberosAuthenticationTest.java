/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.elasticsearch.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.util.MockClock;

/**
 * Unit tests for the Kerberos re-login policy, driven without a KDC through the injectable login
 * and token steps.
 *
 * @author Chris Cranford
 */
@Tag("UnitTests")
public class KerberosAuthenticationTest {

    private static final long START_MS = 1_000_000_000L;
    private static final long TICKET_LIFETIME_MS = TimeUnit.HOURS.toMillis(10);
    private static final String HOST = "es.example.com";

    private final MockClock clock = new MockClock(START_MS);
    private final List<Subject> logins = new ArrayList<>();
    private final List<Subject> tokenSubjects = new ArrayList<>();

    @Test
    void shouldReuseTheSubjectWhileTheTicketIsCurrent() {
        final KerberosAuthentication auth = authentication(this::login, this::token);

        assertThat(auth.negotiateToken(HOST)).isEqualTo("token-1");
        clock.increment(TICKET_LIFETIME_MS - KerberosAuthentication.RELOGIN_MARGIN_MS - 1);
        assertThat(auth.negotiateToken(HOST)).isEqualTo("token-1");

        assertThat(logins).hasSize(1);
        assertThat(tokenSubjects).containsExactly(logins.get(0), logins.get(0));
    }

    @Test
    void shouldLogInAgainBeforeTheTicketExpires() {
        final KerberosAuthentication auth = authentication(this::login, this::token);
        auth.negotiateToken(HOST);

        clock.increment(TICKET_LIFETIME_MS - KerberosAuthentication.RELOGIN_MARGIN_MS);
        assertThat(auth.negotiateToken(HOST)).isEqualTo("token-2");

        assertThat(logins).hasSize(2);
        assertThat(tokenSubjects).containsExactly(logins.get(0), logins.get(1));
    }

    @Test
    void shouldLogInAgainAfterAnExpiredTicketWasRenewed() {
        final KerberosAuthentication auth = authentication(this::login, this::token);
        auth.negotiateToken(HOST);

        // Past the first ticket entirely; the second login issues a ticket that is current again
        // from the new 'now', so no third login follows until it too approaches its end.
        clock.increment(TICKET_LIFETIME_MS + 1);
        auth.negotiateToken(HOST);
        clock.increment(TimeUnit.HOURS.toMillis(1));
        auth.negotiateToken(HOST);

        assertThat(logins).hasSize(2);
    }

    @Test
    void shouldLogInAgainAndRetryOnceWhenTheTokenFails() {
        final AtomicInteger attempts = new AtomicInteger();
        final KerberosAuthentication auth = authentication(this::login, (subject, host) -> {
            tokenSubjects.add(subject);
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("ticket invalidated");
            }
            return "retried";
        });

        assertThat(auth.negotiateToken(HOST)).isEqualTo("retried");

        assertThat(logins).hasSize(2);
        assertThat(tokenSubjects).containsExactly(logins.get(0), logins.get(1));
    }

    @Test
    void shouldReportTheRetryFailureAfterASecondLogin() {
        final KerberosAuthentication auth = authentication(this::login, (subject, host) -> {
            throw new IllegalStateException("kdc unreachable");
        });

        assertThatThrownBy(() -> auth.negotiateToken(HOST))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining(HOST)
                .hasRootCauseMessage("kdc unreachable");

        assertThat(logins).hasSize(2);
    }

    @Test
    void shouldNotLogInProactivelyWithoutATicketGrantingTicket() {
        final KerberosAuthentication auth = authentication(() -> {
            final Subject subject = new Subject();
            logins.add(subject);
            return subject;
        }, this::token);

        auth.negotiateToken(HOST);
        clock.increment(TimeUnit.DAYS.toMillis(30));
        auth.negotiateToken(HOST);

        assertThat(logins).hasSize(1);
    }

    @Test
    void shouldFailConstructionWhenTheLoginFails() {
        assertThatThrownBy(() -> authentication(() -> {
            throw new DebeziumException("Kerberos login failed");
        }, this::token))
                .isInstanceOf(DebeziumException.class)
                .hasMessage("Kerberos login failed");
    }

    private KerberosAuthentication authentication(Supplier<Subject> login, KerberosAuthentication.TokenSource tokens) {
        return new KerberosAuthentication(login, tokens, clock);
    }

    /**
     * A login that issues a ticket-granting ticket valid for {@link #TICKET_LIFETIME_MS} from the
     * clock's current time.
     */
    private Subject login() {
        final long now = clock.currentTimeInMillis();
        final KerberosPrincipal client = new KerberosPrincipal("debezium@EXAMPLE.COM");
        final KerberosPrincipal tgs = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
        final KerberosTicket tgt = new KerberosTicket(new byte[]{ 1 }, client, tgs, new byte[]{ 2 }, 17,
                new boolean[]{ false, false, false, false, false, false, false, false, false, true },
                new Date(now), new Date(now), new Date(now + TICKET_LIFETIME_MS), null, null);
        final Subject subject = new Subject(false, Set.of(client), Set.of(), Set.of(tgt));
        logins.add(subject);
        return subject;
    }

    private String token(Subject subject, String host) {
        tokenSubjects.add(subject);
        return "token-" + (logins.indexOf(subject) + 1);
    }
}
