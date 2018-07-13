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

package org.emmalanguage.labyrinth.operators;

import org.emmalanguage.labyrinth.BagOperatorHost;
import org.emmalanguage.labyrinth.BagOperatorOutputCollector;
import org.emmalanguage.labyrinth.CFLConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class BagOperator<IN, OUT> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperator.class);

	protected BagOperatorOutputCollector<OUT> out;

	protected TypeSerializer<IN> inSer;

	private boolean[] open = new boolean[]{false, false, false};

	protected String name;

	protected BagOperatorHost<IN, OUT> host;

	public void giveHost(BagOperatorHost<IN, OUT> host) {
		this.host = host;
	}

	public void openInBag(int logicalInputId) {
		if (CFLConfig.vlog) LOG.info("openInBag[" + name + "]: logicalInputId: " + logicalInputId);
		assert !open[logicalInputId];
		open[logicalInputId] = true;
	}

	public final void setName(String name) {
		this.name = name;
	}

	public final void giveOutputCollector(BagOperatorOutputCollector<OUT> out) {
		this.out = out;
	}

	public final void giveInputSerializer(TypeSerializer<IN> serializer) { this.inSer = serializer; }

	public void openOutBag() {
		if (CFLConfig.vlog) LOG.info("openOutBag[" + name + "]");
	}

	public void pushInElement(IN e, int logicalInputId) {
		if (CFLConfig.vlog) LOG.info("pushInElement[" + name + "]: e: " + e + " logicalInputId: " + logicalInputId);
		assert open[logicalInputId];
	}

	// Warning: Overriding methods should pay attention to closing the out bag.
	public void closeInBag(int inputId) {
		if (CFLConfig.vlog) LOG.info("closeInBag[" + name + "]: inputId: " + inputId);
		assert open[inputId];
		open[inputId] = false;
	}

}
