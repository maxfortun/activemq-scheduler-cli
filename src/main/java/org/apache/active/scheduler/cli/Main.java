package org.apache.activemq.scheduler.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;

public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class.getName());

	public static void main(String[] args) {
		Main main = new Main();
		try {
			main.run(args);
		} catch (Exception e) {
			logger.error("Failed", e);
			System.exit(1);
		}
	}

	public static final String OPT_SURL = "sb";
	public static final String OPT_SUSER = "su";
	public static final String OPT_SPASS = "sp";

	public static final String OPT_TURL = "tb";
	public static final String OPT_TUSER = "tu";
	public static final String OPT_TPASS = "tp";

	private Options createOptions() {
		Options options = new Options();

        options.addOption(OPT_SURL, "source-broker", true, "Source broker url.");
        options.addOption(OPT_SUSER, "source-user", true, "Source broker username.");
        options.addOption(OPT_SPASS, "source-pass", true, "Source broker password.");

        options.addOption(OPT_TURL, "target-broker", true, "Target broker url.");
        options.addOption(OPT_TUSER, "target-user", true, "Target broker username.");
        options.addOption(OPT_TPASS, "target-pass", true, "Target broker password.");

		return options;
	}

    private ConnectionFactory sourceConnectionFactory;
    private Connection sourceConnection;
    private Session sourceSession;

	private Destination requestBrowse = null;
    private Destination browseDest = null;
	private long browseTimeout = 6000;

	private MessageProducer sourceProducer = null;
	private MessageConsumer sourceBrowser = null;


    private ConnectionFactory targetConnectionFactory;
    private Connection targetConnection;
    private Session targetSession;

    private CommandLine commandLine;

	public void run(String[] args) throws Exception {
		Options options = createOptions();

		if (args.length == 0) {
			HelpFormatter helpFormatter = new HelpFormatter();
			String cmd = ProcessHandle.current().info().commandLine().orElseThrow();
			helpFormatter.printHelp(cmd, options, true);
			System.exit(0);
		}

		CommandLineParser commandLineParser = new DefaultParser();

		commandLine = commandLineParser.parse(options, args);

		setupSource();
		processSource();
	}

	private void setupSource() throws Exception {
		sourceConnectionFactory = new ActiveMQConnectionFactory(commandLine.getOptionValue(OPT_SUSER), commandLine.getOptionValue(OPT_SPASS), commandLine.getOptionValue(OPT_SURL));
		sourceConnection = sourceConnectionFactory.createConnection();

		sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        requestBrowse = sourceSession.createTopic(ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION);
        browseDest = sourceSession.createTemporaryQueue();

        sourceConnection.start();

        sourceProducer = sourceSession.createProducer(requestBrowse);
        sourceBrowser = sourceSession.createConsumer(browseDest);
		logger.debug("Source AMQ setup Ok");
	}

	private javax.jms.Message createBrowseRequest() throws Exception {
        javax.jms.Message request = sourceSession.createMessage();
        request.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION, ScheduledMessage.AMQ_SCHEDULER_ACTION_BROWSE);
        request.setJMSReplyTo(browseDest);
        sourceProducer.send(request);
        return request;
    }

	private void processSource() throws Exception {
		javax.jms.Message request = createBrowseRequest();
		javax.jms.Message scheduledMessage;

		while ((scheduledMessage = sourceBrowser.receive(browseTimeout)) != null) {
			logger.debug(scheduledMessage.toString());
		}

	}
}


