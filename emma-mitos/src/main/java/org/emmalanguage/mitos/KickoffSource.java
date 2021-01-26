/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.emmalanguage.mitos;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.emmalanguage.mitos.util.Unit;
import eu.stratosphere.mitos.CFLManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: We couldn't directly tell the CFLManager from the driver, because it is important
// that these calls to the CFLManager happen on the TMs.
public class KickoffSource extends RichSourceFunction<Unit> {

	private static final Logger LOG = LoggerFactory.getLogger(KickoffSource.class);

	private final int[] kickoffBBs;
	private final CFLConfig cflConfig;
	private int terminalBBId = -2;

	public KickoffSource(int... kickoffBBs) {
		this.kickoffBBs = kickoffBBs;
		this.cflConfig = CFLConfig.getInstance();
		this.terminalBBId = cflConfig.terminalBBId;
		assert this.terminalBBId >= 0 : "CFLConfig has to be set before creating KickoffSource";
	}

	@Override
	public void run(SourceContext sourceContext) throws Exception {
		LOG.info("KickoffSource kicking off");
		//CFLManager cflManager = CFLManager.getSing();
		CFLManager cflManager = ((StreamingRuntimeContext)getRuntimeContext()).getCFLManager();

		//cflManager.resetCFL(); // I moved this to TaskManager.scala
		cflManager.specifyTerminalBB(terminalBBId);

		assert cflConfig.numToSubscribe != -10;
		cflManager.specifyNumToSubscribe(cflConfig.numToSubscribe);

		if (cflConfig.shouldEnableCheckpointing) cflManager.setCheckpointingEnabled(true);
		if (cflConfig.checkpointInterval != CFLConfig.checkpointIntervalNotSet) cflManager.setCheckpointInterval(cflConfig.checkpointInterval);
		if (cflConfig.checkpointDir != null) cflManager.setCheckpointDir(cflConfig.checkpointDir);
		cflManager.initSnapshottingLocal(kickoffBBs);

		//cflManager.appendToCFL(kickoffBBs); // Moved to initSnapshottingRemote
	}

	@Override
	public void cancel() {

	}
}
