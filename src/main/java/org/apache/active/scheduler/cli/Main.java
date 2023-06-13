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

import java.io.File;

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

	private static final String OPT_SURL = "sb";
	private static final String OPT_SUSER = "su";
	private static final String OPT_SPASS = "sp";

	private static final String OPT_TURL = "tb";
	private static final String OPT_TUSER = "tu";
	private static final String OPT_TPASS = "tp";

	private static final String OPT_TDIR = "td";
	private static final String OPT_TID = "tid";


	private Options createOptions() {
		Options options = new Options();

        options.addOption(OPT_SURL, "source-broker", true, "Source broker url.");
        options.addOption(OPT_SUSER, "source-user", true, "Source broker username.");
        options.addOption(OPT_SPASS, "source-pass", true, "Source broker password.");

        options.addOption(OPT_TURL, "target-broker", true, "Target broker url.");
        options.addOption(OPT_TUSER, "target-user", true, "Target broker username.");
        options.addOption(OPT_TPASS, "target-pass", true, "Target broker password.");

        options.addOption(OPT_TDIR, "target-dir", true, "Target directory.");
        options.addOption(OPT_TID, "target-id", true, "Target id property field. Default: "+idProperty);

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

	private int targetCount = 0;
	private String idProperty = "scheduledJobId";
	private File targetDir = null;

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

		setupTargetDir();
		setupTargetBroker();

		setupSourceBroker();
		processSource();
	}

	private void setupTargetDir() throws Exception {
		if(!commandLine.hasOption(OPT_TDIR)) {
			logger.info("Target directory not specified");
			return;
		}
		targetDir = new File(commandLine.getOptionValue(OPT_TDIR));
		if(!targetDir.isDirectory()) {
			targetDir.mkdirs();
		}
		logger.info("Target dir: "+targetDir);
		targetCount++;
	}

	private void setupTargetBroker() throws Exception {
		if(!commandLine.hasOption(OPT_TURL)) {
			logger.info("Target broker not specified");
			return;
		}
		try {
			targetConnectionFactory = new ActiveMQConnectionFactory(commandLine.getOptionValue(OPT_TUSER), commandLine.getOptionValue(OPT_TPASS), commandLine.getOptionValue(OPT_TURL));
			targetConnection = targetConnectionFactory.createConnection();
			targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		} catch(Exception e) {
			logger.error("Failed to setup targety broker.", e);
		}
		logger.info("Target broker connected.");
		targetCount++;
	}

	private void setupSourceBroker() throws Exception {
		sourceConnectionFactory = new ActiveMQConnectionFactory(commandLine.getOptionValue(OPT_SUSER), commandLine.getOptionValue(OPT_SPASS), commandLine.getOptionValue(OPT_SURL));
		sourceConnection = sourceConnectionFactory.createConnection();

		sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        requestBrowse = sourceSession.createTopic(ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION);
        browseDest = sourceSession.createTemporaryQueue();

        sourceConnection.start();

        sourceProducer = sourceSession.createProducer(requestBrowse);
        sourceBrowser = sourceSession.createConsumer(browseDest);
		logger.info("Source broker connected");
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
		javax.jms.Message sourceMessage;

		while ((sourceMessage = sourceBrowser.receive(browseTimeout)) != null) {
			processSourceMessage(sourceMessage);
		}
		logger.info("Done");
		shutdownTargetBroker();
		shutdownSourceBroker();
	}

	private void shutdownTargetBroker() throws Exception {
	}

	private void shutdownSourceBroker() throws Exception {
		sourceProducer.close();
		sourceBrowser.close();
		sourceSession.close();
		sourceConnection.close();
	}

	private void processSourceMessage(javax.jms.Message sourceMessage) throws Exception {
		logger.debug(sourceMessage.toString());
	}
}


