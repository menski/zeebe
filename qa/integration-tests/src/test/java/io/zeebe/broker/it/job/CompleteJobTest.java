package io.zeebe.broker.it.job;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.client.ClientProperties;
import io.zeebe.client.api.clients.JobClient;
import io.zeebe.client.api.events.JobEvent;
import io.zeebe.client.cmd.BrokerErrorException;

public class CompleteJobTest
{

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule(() ->
    {
        final Properties p = new Properties();
        p.setProperty(ClientProperties.CLIENT_REQUEST_TIMEOUT_SEC, "3");
        return p;
    }, true);

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public Timeout testTimeout = Timeout.seconds(15);

    @Test
    @Ignore
    public void testCannotCompleteUnlockedJob()
    {
        // given
        final JobClient jobClient = clientRule.getClient().topicClient().jobClient();

        final JobEvent job = jobClient.newCreateCommand()
            .jobType("bar")
            .payload("{}")
            .send()
            .join();

        // then
        thrown.expect(BrokerErrorException.class);
        thrown.expectMessage("Job does not exist or is not locked");

        // when
        jobClient.newCompleteCommand(job)
            .send()
            .join();
    }
}
