package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueAclsInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueueHandlerHiveDriverRunHook implements HiveDriverRunHook {

	private static final String QUEUE_NAME_PROPERTY = "mapred.job.queue.name";

	private static final Log LOG = LogFactory
			.getLog(QueueHandlerHiveDriverRunHook.class);

	@Override
	public void postDriverRun(HiveDriverRunHookContext context)
			throws Exception {
	}

	@Override
	public void preDriverRun(HiveDriverRunHookContext context) throws Exception {

		HiveConf config = (HiveConf) context.getConf();
		String queue = config.get(QueueHandlerHiveDriverRunHook.QUEUE_NAME_PROPERTY);

		// if queue name is specified as default, try to find a better qualified
		// queue
		if ("default".equals(queue)) {
			String newQueue = getQualifiedQueue(context.getConf(), queue);
			if (newQueue != null && !newQueue.equalsIgnoreCase(queue)) {
				config.set(QueueHandlerHiveDriverRunHook.QUEUE_NAME_PROPERTY, newQueue);
				LOG.info("queue name overriden to " + queue
						+ " From default for the user " + config.getUser());
			}
		}
	}

	private String getQualifiedQueue(Configuration config, String queueInp) {

		String queue = queueInp;
		JobConf job = new JobConf(config);
		JobClient jobClient;
		try {
			jobClient = new JobClient(job);
			QueueAclsInfo[] infos = jobClient.getQueueAclsForCurrentUser();

			for (QueueAclsInfo info : infos) {
				String name = info.getQueueName().toLowerCase();
				if (name != null && name.indexOf("default") == -1) {
					LOG.debug("Checking acl info for queue:" + name);
					boolean qualified = false;
					String[] ops = info.getOperations();
					LOG.debug("Count Allowed operations:" + ops.length);
					
					if (ops != null && ops.length > 0) {
						for (String op : ops) {
							if ("SUBMIT_APPLICATIONS".equals(op)) {
								qualified = true;
								break;
							}
						}
					}

					if (qualified) {
						queue = name;
						break;
					}
				}
			}
		} catch (IOException e) {
			//Do Nothing. Ignore the error and let query run in the default queue
			e.printStackTrace();
		}
		return queue;
	}
}
