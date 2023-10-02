package org.apache.activemq.scheduler.cp;

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
import org.apache.activemq.command.ActiveMQMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
	private static final String OPT_STIMEOUT = "st";
	private static final String OPT_SRM = "srm";

	private static final String OPT_TURL = "tb";
	private static final String OPT_TUSER = "tu";
	private static final String OPT_TPASS = "tp";

	private static final String OPT_TDIR = "td";
	private static final String OPT_TID = "tid";

	private static final String OPT_DR = "dr";

	private static final String OPT_PF = "pf";


	private Options createOptions() {
		Options options = new Options();

		options.addOption(OPT_SURL, "source-broker", true, "Source broker url.");
		options.addOption(OPT_SUSER, "source-user", true, "Source broker username.");
		options.addOption(OPT_SPASS, "source-pass", true, "Source broker password.");
		options.addOption(OPT_STIMEOUT, "source-timeout", true, "Source timeout. Default: "+sourceTimeout);
		options.addOption(OPT_SRM, "source-rm", true, "Remove from source. Values: never, always, success, error. Default: "+sourceRM);

		options.addOption(OPT_TURL, "target-broker", true, "Target broker url.");
		options.addOption(OPT_TUSER, "target-user", true, "Target broker username.");
		options.addOption(OPT_TPASS, "target-pass", true, "Target broker password.");

		options.addOption(OPT_TDIR, "target-dir", true, "Target directory.");
		options.addOption(OPT_TID, "target-id", true, "Target id property field. Default: "+idProperty);

		options.addOption(OPT_DR, "dry-run", false, "Do not alter broker states by either forwarding to target or removing from source.");
		options.addOption(OPT_PF, "property-filter", true, "Property filter. e.g. property=regex");

		return options;
	}

	private ConnectionFactory sourceConnectionFactory;
	private Connection sourceConnection;
	private Session sourceSession;

	private Destination browseReplyToDestination = null;
	private long sourceTimeout = 6000;
	private String sourceRM = "never";

	private MessageProducer sourceProducer = null;
	private MessageConsumer sourceConsumer = null;


	private ConnectionFactory targetConnectionFactory;
	private Connection targetConnection;
	private Session targetSession;
	private Map<String, MessageProducer> targetProducers = new HashMap<>();

	private CommandLine commandLine;

	private int targetCount = 0;
	private String idProperty = "originalScheduledJobId";
	private File targetDir = null;

	private boolean dryRun = false;
	private Map<String, List<String>> propertyFilters = new HashMap<>();

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

		setupOptions();

		setupPropertyFilters();

		setupTargetDir();

		setupTargetBroker();

		setupSourceBroker();

		processSource();
	}

	private void setupOptions() throws Exception {
		if(commandLine.hasOption(OPT_DR)) {
			dryRun = true;
		}
		logger.debug("Dry run: {}", dryRun);
	}

	private void setupPropertyFilters() throws Exception {
		if(!commandLine.hasOption(OPT_PF)) {
			logger.info("Property filters not specified");
			return;
		}

		String[] filters = commandLine.getOptionValues(OPT_PF);

		for(String filter : filters) {
			int indexOfSep = filter.indexOf("=");
			String propertyName = filter.substring(0, indexOfSep);
			String propertyRE = filter.substring(indexOfSep+1);
			logger.debug("Property filter: "+propertyName+" = "+propertyRE);

			List<String> patterns = propertyFilters.get(propertyName);
			if(patterns == null) {
				patterns = new ArrayList<String>();
				propertyFilters.put(propertyName, patterns);
			}

			patterns.add(propertyRE);
		}
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
		logger.info("Target dir: "+targetDir.getAbsolutePath());
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
		if(commandLine.hasOption(OPT_STIMEOUT)) {
			sourceTimeout = Long.parseLong(commandLine.getOptionValue(OPT_STIMEOUT));
		}
		logger.debug("Source timeout: "+sourceTimeout);

		if(commandLine.hasOption(OPT_SRM)) {
			sourceRM = commandLine.getOptionValue(OPT_SRM).toLowerCase();
		}
		logger.debug("Remove from source: {}", sourceRM);

		sourceConnectionFactory = new ActiveMQConnectionFactory(commandLine.getOptionValue(OPT_SUSER), commandLine.getOptionValue(OPT_SPASS), commandLine.getOptionValue(OPT_SURL));
		sourceConnection = sourceConnectionFactory.createConnection();

		sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination browseRequestDestination = sourceSession.createTopic(ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION);
		browseReplyToDestination = sourceSession.createTemporaryQueue();

		sourceConnection.start();

		sourceProducer = sourceSession.createProducer(browseRequestDestination);
		sourceConsumer = sourceSession.createConsumer(browseReplyToDestination);

		logger.info("Source broker connected");
	}

	private void createBrowseRequest() throws Exception {
		Message request = sourceSession.createMessage();
		request.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION, ScheduledMessage.AMQ_SCHEDULER_ACTION_BROWSE);
		request.setJMSReplyTo(browseReplyToDestination);
		sourceProducer.send(request);
	}

	private void processSource() throws Exception {
		createBrowseRequest();

		ActiveMQMessage message;
		while ((message = (ActiveMQMessage)sourceConsumer.receive(sourceTimeout)) != null) {
			processSourceMessage(message);
		}

		logger.info("Done");

		shutdownTargetBroker();
		shutdownSourceBroker();
	}

	private void shutdownTargetBroker() throws Exception {
		if(null == targetConnection) {
			return;
		}

		for(MessageProducer targetProducer : targetProducers.values()) {
			targetProducer.close();
		}

		targetSession.close();
		targetConnection.close();
	}

	private void shutdownSourceBroker() throws Exception {
		sourceProducer.close();
		sourceConsumer.close();
		sourceSession.close();
		sourceConnection.close();
	}

	private void processSourceMessage(ActiveMQMessage message) throws Exception {
		if(!passFilters(message)) {
			logger.debug("Filtered out: "+message.toString());
			return;
		}

		logger.debug("Processing: "+message.toString());

		ActiveMQMessage fixedMessage = fixMessage(message);
		logger.debug("Fixed: "+fixedMessage.toString());

		int errors = forwardToDir(fixedMessage);
		errors += forwardToTargetBroker(fixedMessage);
		removeFromSource(message, errors);
	}

	private int forwardToDir(ActiveMQMessage message) throws Exception {
		try {
			forwardToDirImpl(message);
			return 0;
		} catch(Exception e) {
			logger.warn("forwardToDir: {} ", message, e);
			return 1;
		}
	}

	private void forwardToDirImpl(ActiveMQMessage message) throws Exception {
		if(null == targetDir) {
			return;
		}

		String id = message.getStringProperty(idProperty);
		File messageDir = new File(targetDir, id);
		logger.debug(messageDir.toString());
		messageDir.mkdir();

		storeMessage(messageDir, message);
		storeProperties(messageDir, message);
		storeBody(messageDir, message);
	}

	private void storeMessage(File messageDir, ActiveMQMessage message) throws Exception {
		File messageFile = new File(messageDir, "message");
		FileOutputStream fileOutputStream = new FileOutputStream(messageFile);
		fileOutputStream.write(message.toString().getBytes());
		fileOutputStream.close();
		fileOutputStream = null;
	}

	private void storeProperties(File messageDir, ActiveMQMessage message) throws Exception {
		Properties properties = new Properties();
		for (Enumeration<String> e = message.getPropertyNames(); e.hasMoreElements();) {
			String name = e.nextElement();
			String value = message.getStringProperty(name);
			properties.setProperty(name, value);
		}

		File propertiesFile = new File(messageDir, "properties");
		FileOutputStream fileOutputStream = new FileOutputStream(propertiesFile);
		properties.store(fileOutputStream, "Properties");
		fileOutputStream.close();
		fileOutputStream = null;
	}

	private void storeBody(File messageDir, ActiveMQMessage message) throws Exception {
		logger.debug("JMSType: "+message.getJMSType());
		if(message.isBodyAssignableTo(String.class)) {
			storeStringBody(messageDir, message);
		} else if(message.isBodyAssignableTo(Map.class)) {
			storeMapBody(messageDir, message);
		} else if(message.isBodyAssignableTo(byte[].class)) {
			storeBytesBody(messageDir, message);
		} else if(message.isBodyAssignableTo(Serializable.class)) {
			storeObjectBody(messageDir, message);
		} else {
			logger.warn("Unknown message type: "+message.toString());
		}
	}

	private void storeStringBody(File messageDir, ActiveMQMessage message) throws Exception {
		File bodyFile = new File(messageDir, "body.string");
		logger.debug("Body: "+bodyFile.getAbsolutePath());
		String body = message.getBody(String.class);
		FileOutputStream fileOutputStream = new FileOutputStream(bodyFile);
		fileOutputStream.write(body.getBytes());
		fileOutputStream.close();
		fileOutputStream = null;
	}

	private void storeMapBody(File messageDir, ActiveMQMessage message) throws Exception {
		File bodyFile = new File(messageDir, "body.map");
		logger.debug("Body: "+bodyFile.getAbsolutePath());
		Map body = message.getBody(Map.class);
		FileOutputStream fileOutputStream = new FileOutputStream(bodyFile);

		ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
		objectOutputStream.writeObject(body);
		objectOutputStream.close();
		objectOutputStream = null;

		fileOutputStream.close();
		fileOutputStream = null;
	}

	private void storeBytesBody(File messageDir, ActiveMQMessage message) throws Exception {
		File bodyFile = new File(messageDir, "body.bytes");
		logger.debug("Body: "+bodyFile.getAbsolutePath());
		byte[] body = message.getBody(byte[].class);
		FileOutputStream fileOutputStream = new FileOutputStream(bodyFile);
		fileOutputStream.write(body);
		fileOutputStream.close();
		fileOutputStream = null;
	}

	private void storeObjectBody(File messageDir, ActiveMQMessage message) throws Exception {
		File bodyFile = new File(messageDir, "body.object");
		logger.debug("Body: "+bodyFile.getAbsolutePath());
		Object body = message.getBody(Object.class);
		FileOutputStream fileOutputStream = new FileOutputStream(bodyFile);

		ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
		objectOutputStream.writeObject(body);
		objectOutputStream.close();
		objectOutputStream = null;

		fileOutputStream.close();
		fileOutputStream = null;
	}

	private MessageProducer getTargetProducer(ActiveMQMessage message) throws Exception {
		Destination destination = message.getOriginalDestination();
		
		if(null == destination) {
			logger.warn("Could not find destination "+message);
			return null;
		}

		MessageProducer targetProducer = targetProducers.get(destination.toString());
		if(null == targetProducer) {
			targetProducer = targetSession.createProducer(destination);
			targetProducers.put(destination.toString(), targetProducer);
			logger.debug("Target destination: "+destination);
		}
		return targetProducer;
	}

	private int forwardToTargetBroker(ActiveMQMessage message) throws Exception {
		try {
			forwardToTargetBrokerImpl(message);
			return 0;
		} catch(Exception e) {
			logger.warn("forwardToTargetBroker: {} ", message, e);
			return 1;
		}
	}

	private ActiveMQMessage fixMessage(ActiveMQMessage sourceMessage) throws Exception {
		ActiveMQMessage message = (ActiveMQMessage)sourceMessage.copy();
		message.setDestination(message.getOriginalDestination());
		String originalScheduledJobId = message.getStringProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
		message.setProperty("originalScheduledJobId", originalScheduledJobId);
		message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_ID);

		String delayString = message.getStringProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY);
		if(null != delayString) {
			long delay = Long.parseLong(delayString);
			long timestamp = message.getTimestamp();
			long now = new Date().getTime();
			long delayShift = now - timestamp;
			long shiftedDelay = delay - delayShift;
			message.setProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, ""+shiftedDelay);
			logger.debug("Shifting delay from "+timestamp+"+"+delay+"="+new Date(timestamp+delay)+" to "+now+"+"+shiftedDelay+"="+new Date(now+shiftedDelay));
		}

		return message;
	}

	private void forwardToTargetBrokerImpl(ActiveMQMessage message) throws Exception {
		if(null == targetConnection) {
			return;
		}

		MessageProducer targetProducer = getTargetProducer(message);
		if(null == targetProducer) {
			return;
		}

		if(dryRun) {
			logger.debug("forwardToTargetBroker(DRY RUN): {}", message); 
			return;
		}

		logger.debug("forwardToTargetBroker: {}", message); 
		targetProducer.send(message);
	}

	private void removeFromSource(ActiveMQMessage message, int errors) throws Exception {
		if("always".equals(sourceRM)) {
			removeFromSource(message);
			return;
		}

		if("success".equals(sourceRM) && errors == 0) {
			removeFromSource(message);
			return;
		}

		if("error".equals(sourceRM) && errors > 0) {
			removeFromSource(message);
			return;
		}
	}

	private void removeFromSource(ActiveMQMessage message) throws Exception {
		Message request = sourceSession.createMessage();
		request.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION, ScheduledMessage.AMQ_SCHEDULER_ACTION_REMOVE);
		request.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_ID, message.getStringProperty(ScheduledMessage.AMQ_SCHEDULED_ID));
		if(dryRun) {
			logger.debug("removeFromSource(DRY RUN): {}", request); 
			return;
		}
		logger.debug("removeFromSource: {}", request); 
		sourceProducer.send(request);
	}

	private boolean passFilters(ActiveMQMessage message) throws Exception {
		if(!passPropertyFilters(message)) {
			return false;
		}

		return true;
	}

	private boolean passPropertyFilters(ActiveMQMessage message) throws Exception {
		if(propertyFilters.size() == 0) {
			return true;
		}

		for(String propertyName : propertyFilters.keySet()) {
			String propertyValue = message.getStringProperty(propertyName);
			List<String> patterns = propertyFilters.get(propertyName);
			for(String pattern : patterns) {
				if(!propertyValue.matches(pattern)) {
					logger.debug(propertyName+" = " + propertyValue+ " !~ " + pattern);
					return false;
				}
			}
		}

		return true;
	}
}


