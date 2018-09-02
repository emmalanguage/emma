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

package org.emmalanguage.labyrinth;


import eu.stratosphere.labyrinth.BagID;
import eu.stratosphere.labyrinth.CFLCallback;
import eu.stratosphere.labyrinth.CFLManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.emmalanguage.labyrinth.operators.ReusingBagOperator;
import org.emmalanguage.labyrinth.operators.BagOperator;
import org.emmalanguage.labyrinth.operators.DontThrowAwayInputBufs;
import org.emmalanguage.labyrinth.partitioners.Broadcast;
import org.emmalanguage.labyrinth.partitioners.Partitioner;
import org.emmalanguage.labyrinth.util.SerializedBuffer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.InputParaSettable;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.labyrinth.NoAutoClose;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class BagOperatorHost<IN, OUT>
		extends AbstractStreamOperator<ElementOrEvent<OUT>>
		implements OneInputStreamOperator<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			InputParaSettable<ElementOrEvent<IN>,ElementOrEvent<OUT>>,
			NoAutoClose,
			Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BagOperatorHost.class);

	protected BagOperator<IN,OUT> op;
	public int bbId;
	private int inputParallelism = -1;
	public String name;
	private int terminalBBId = -2;
	private CFLConfig cflConfig;
	public int opID = -1;
	public TypeSerializer<IN> inSer;

	// ---------------------- Initialized in setup (i.e., on TM):

	public short subpartitionId = -25;
	private short para;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	protected ArrayList<Input> inputs;

	// ----------------------

	protected List<Integer> latestCFL; // note that this is the same object instance as in CFLManager
	protected Queue<Integer> outCFLSizes; // if not empty, then we are working on the first one; if empty, then we are not working

	public ArrayList<Out> outs = new ArrayList<>(); // conditional and normal outputs

	private volatile boolean terminalBBReached;

	private HashSet<BagID> notifyCloseInputs = new HashSet<>();
	private HashSet<BagID> notifyCloseInputEmpties = new HashSet<>();

	private boolean consumed = false;

	private HashSet<Tuple2<Integer, Integer>> inputUses = new HashSet<>(); // (inputID, cflSize)

	private boolean shouldLogStart;

	private int barrierAllReachedCFLSize = 1;

	private boolean workInProgress = false;

	private ExecutorService es;

	private final boolean reuseInputs;

	public BagOperatorHost(BagOperator<IN,OUT> op, int bbId, int opID, TypeSerializer<IN> inSer) {
		this.op = op;
		this.bbId = bbId;
		this.inputs = new ArrayList<>();
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		this.cflConfig = CFLConfig.getInstance();
		this.reuseInputs = this.cflConfig.reuseInputs;
		assert this.terminalBBId >= 0;
		this.opID = opID;
		this.inSer = inSer;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	// Does not set op
	protected BagOperatorHost(int bbId, int opID, TypeSerializer<IN> inSer) {
		this.bbId = bbId;
		this.inputs = new ArrayList<>();
		this.terminalBBId = CFLConfig.getInstance().terminalBBId;
		this.cflConfig = CFLConfig.getInstance();
		this.reuseInputs = this.cflConfig.reuseInputs;
		assert this.terminalBBId >= 0;
		this.opID = opID;
		this.inSer = inSer;
		// warning: this runs in the driver, so we shouldn't access CFLManager here
	}

	public BagOperatorHost<IN,OUT> addInput(int id, int bbId, boolean inputInSameBlock, int opID) {
		assert id == inputs.size();
		inputs.add(new Input(id, bbId, inputInSameBlock, opID));
		return this;
	}

	@Override
	public void setInputPara(int p) {
		this.inputParallelism = p;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		this.op.setName(name);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ElementOrEvent<OUT>>> output) {
		super.setup(containingTask, config, output);

		this.subpartitionId = (short)getRuntimeContext().getIndexOfThisSubtask();

		assert getRuntimeContext().getNumberOfParallelSubtasks() <= Short.MAX_VALUE;
		this.para = (short)getRuntimeContext().getNumberOfParallelSubtasks();

		//LOG.info("subpartitionId [" + name + "]: " + subpartitionId);

		if (inputParallelism == -1) {
			throw new RuntimeException("inputParallelism is not set. Use either the LabyNode API or .bt instead of .transform!");
		}

		for(Input inp: inputs) {
			inp.inputSubpartitions = new InputSubpartition[inputParallelism];
			for (int i = 0; i < inp.inputSubpartitions.length; i++) {
				inp.inputSubpartitions[i] = new InputSubpartition<>(inSer, !(op instanceof DontThrowAwayInputBufs));
			}
		}

		outCFLSizes = new ArrayDeque<>();

		terminalBBReached = false;

		op.giveOutputCollector(new MyCollector());

		op.giveHost(this);

		op.giveInputSerializer(inSer);

		es = Executors.newSingleThreadExecutor();

		//cflMan = CFLManager.getSing();
		cflMan = getRuntimeContext().getCFLManager();

		cflMan.specifyTerminalBB(terminalBBId);
		cflMan.specifyNumToSubscribe(cflConfig.numToSubscribe);
	}

	@Override
	public void open() throws Exception {
		super.open();

		Thread.sleep(100); // this is a workaround for the buffer pool destroyed error

		cb = new MyCFLCallback();
		cflMan.subscribe(cb);
	}

	private int getMul() {
		int mul = -1;
		if (op instanceof MutableBagCC.MutableBagOperator) {
			switch (((MutableBagCC.MutableBagOperator) op).inpID) {
				case -1: // toBag
					mul = 1;
					break;
				case 0: // toMutable
					mul = 10;
					break;
				case 1: // join
					mul = 100;
					break;
				case 2: // update
					mul = 1000;
					break;
				default:
					assert false;
			}
		} else {
			mul = 1;
		}
		return mul;
	}

	private void notifyLogStart() {
		if (shouldLogStart) {
			shouldLogStart = false;
			if (CFLConfig.logStartEnd) {
				LOG.info("=== " + System.currentTimeMillis() + " S " + outCFLSizes.peek() + " " + opID * getMul());
			}
		}
	}

	@Override
	synchronized public void processElement(StreamRecord<ElementOrEvent<IN>> streamRecord) throws Exception {

		if (CFLConfig.vlog) LOG.info("Operator {" + name + "}[" + subpartitionId +"] processElement " + streamRecord.getValue());

		ElementOrEvent<IN> eleOrEvent = streamRecord.getValue();
		if (inputs.size() == 1) {
			assert eleOrEvent.logicalInputId == -1 || eleOrEvent.logicalInputId == 0;
			eleOrEvent.logicalInputId = 0; // This is to avoid having to have an extra map to set this for even one-input operators
		}
		assert eleOrEvent.logicalInputId != -1; // (there is an extra map needed, which fills this (if not one-input))
		Input input = inputs.get(eleOrEvent.logicalInputId);
		InputSubpartition<IN> sp = input.inputSubpartitions[eleOrEvent.subPartitionId];

		if (eleOrEvent.element != null) {
			IN ele = eleOrEvent.element;
			sp.buffers.get(sp.buffers.size()-1).elements.add(ele);
			if(!sp.damming) {
				consumed = true;
				notifyLogStart();
				op.pushInElement(ele, eleOrEvent.logicalInputId);
				sp.buffers.get(sp.buffers.size()-1).elements.consumeStarted = true;
			}
		} else {

			assert eleOrEvent.event != null;
			ElementOrEvent.Event ev = eleOrEvent.event;
			switch (eleOrEvent.event.type) {
				case START:
					assert eleOrEvent.event.assumedTargetPara == para;
					assert sp.status == InputSubpartition.Status.CLOSED;
					sp.status = InputSubpartition.Status.OPEN;
					sp.addNewBuffer(ev.bagID);
					assert input.opID == ev.bagID.opID;
					//assert input.currentBagID == null || input.currentBagID.equals(ev.bagID); // I had to comment this out, because the null resetting was removed, because it was at a bad spot
					//assert input.currentBagID.equals(ev.bagID); // This does not work, because it happens that activateLogicalInput has not yet run when a start arrives
					// The next assignment doesn't work, because notifyCloseInput would think that it is already activated.
					// But it is possible that it will be activated only later (and then we of course check whether we have already received a notifyCloseInput for it).
					//input.currentBagID = ev.bagID;
					// Note: Sometimes the buffer would not be really needed: we could check some tricky condition on the
					// control flow graph, but this is not so important for the experiments in the paper.
					if(input.inputCFLSize != -1) {
						if(input.inputCFLSize == ev.bagID.cflSize){ // It is just what we need for the current out bag
							sp.damming = false;
						} else { // It doesn't match our current out bag
							sp.damming = true;
						}
					} else { // We are not working on any out bag at the moment
						sp.damming = true;
					}
					break;
				case END:
					assert eleOrEvent.event.assumedTargetPara == para;
					assert sp.status == InputSubpartition.Status.OPEN;
					sp.status = InputSubpartition.Status.CLOSED;
					InputSubpartition.Buffer lastBuffer = sp.buffers.get(sp.buffers.size()-1);
					cflMan.consumedLocal(lastBuffer.bagID,lastBuffer.elements.size(),subpartitionId,opID);
					break;
				default:
					assert false;
					break;
			}

		}
	}

	protected void outCFLSizesRemove() {
		outCFLSizes.remove();
	}

	private class MyCollector implements BagOperatorOutputCollector<OUT> {

		private int numElements = 0;

		@Override
		public void collectElement(OUT e) {
			numElements++;
			//assert outs.size() != 0; // If the operator wants to emit an element but there are no outs, then we just probably forgot to call .out()
			for(Out o: outs) {
				o.collectElement(e);
			}
		}

		@Override
		public void closeBag() {
			for(Out o: outs) {
				o.closeBag();
			}

			if (CFLConfig.logStartEnd) {
				LOG.info("=== " + System.currentTimeMillis() + " E " + outCFLSizes.peek() + " " + opID * getMul());
			}

			ArrayList<BagID> inputBagIDs = new ArrayList<>();
			for (Input inp: inputs) {
				// This assert can fail when we have a kind of a short-circuit-style operator, which sometimes closes
				// the output bag before the input bag started on all inputs, but elements will come from there later.
				//assert inp.currentBagID != null; // Doesn't work for PhiNode
				if (inp.activeFor.contains(outCFLSizes.peek())) {
					assert inp.currentBagID != null;
					inputBagIDs.add(inp.currentBagID);
				}

				// This can also fail for such short-circuit-style operators which don't wait for input bag closures before closing out.
				assert inp.inputCFLSize == -1;
			}
			BagID[] inputBagIDsArr = new BagID[inputBagIDs.size()];
			int i = 0;
			for (BagID b: inputBagIDs) {
				inputBagIDsArr[i++] = b;
			}

			BagID outBagID = new BagID(outCFLSizes.peek(), opID);
			if (numElements > 0 || consumed || inputBagIDs.size() == 0) {
				// In the inputBagIDs.size() == 0 case we have to send, because in this case checkForClosingProduced expects from everywhere
				// (because of the s.inputs.size() == 0) at its beginning)
				if (!(BagOperatorHost.this instanceof MutableBagCC && ((MutableBagCC.MutableBagOperator)op).inpID == 2)) {
					numElements = correctBroadcast(numElements);
					cflMan.producedLocal(outBagID, inputBagIDsArr, numElements, para, subpartitionId, opID);
				}
			}

			numElements = 0;

			outCFLSizesRemove();
			workInProgress = false;
			if(outCFLSizes.size() > 0) { // if there is pending work
				if (CFLConfig.vlog) LOG.info("Out.closeBag starting a new out bag {" + name + "}");
				// Note: outs' buffers will be discarded from this, but this is not a problem, because everything that has to be sent
				// has been already sent
				startOutBagCheckBarrier();
			} else {
				if (CFLConfig.vlog) LOG.info("Out.closeBag not starting a new out bag {" + name + "}");
				if (terminalBBReached) { // if there is no pending work, and none would come later
					cflMan.unsubscribe(cb);
					es.shutdown();
				}
			}
		}

		@Override
		public void appendToCfl(int bbId) {
			cflMan.appendToCFL(bbId);
		}

		private int correctBroadcast(int numElements) {
			if (outs.size() > 0 && outs.get(0).partitioner instanceof Broadcast) {
				for (Out o: outs) {
					// We have a limitation that if an output is a broadcast output, then all of them has to be broadcast
					assert o.partitioner instanceof Broadcast;
				}
				return numElements * outs.get(0).partitioner.targetPara;
			} else {
				return numElements;
			}
		}
	}

	// i: buffer index in inputSubpartitions
	synchronized private void giveBufferToBagOperator(InputSubpartition<IN> sp, int i, int logicalInputId) {
		consumed = true;
		notifyLogStart();
		try {
			for (IN e : sp.buffers.get(i).elements) {
				op.pushInElement(e, logicalInputId);
			}
		} catch (NullPointerException npe) {
			throw new RuntimeException();
		}
	}

	protected void chooseOuts() {
		for (Out out: outs) {
			out.active = true;
		}
	}

	synchronized private void startOutBagCheckBarrier() {
		if(!CFLManager.barrier) {
			if (CFLConfig.vlog)
				LOG.info("[" + name + "] CFLCallback.notify starting an out bag");
			startOutBag();
		} else {
			assert !outCFLSizes.isEmpty();
			int outCFLSize = outCFLSizes.peek();
			int plus = 3; // This is needed because in some cases the next iteration step is not one block away. For example, in ClickCount we have an if in the loop body, so it's 3.
			if (outCFLSize <= barrierAllReachedCFLSize + plus) {
				if (CFLConfig.vlog) LOG.info("startOutBagCheckBarrier starting a bag. {" + name + "} outCFLSize = " + outCFLSize + ", barrierAllReachedCFLSize + plus = " + (barrierAllReachedCFLSize + plus));
				startOutBag();
			} else {
				if (CFLConfig.vlog) LOG.info("startOutBagCheckBarrier not starting a bag. {" + name + "} outCFLSize = " + outCFLSize + ", barrierAllReachedCFLSize + plus = " + (barrierAllReachedCFLSize + plus));
			}
		}
	}

	synchronized private void startOutBag() {
		assert !outCFLSizes.isEmpty();
		Integer outCFLSize = outCFLSizes.peek();

		assert latestCFL.get(outCFLSize - 1).equals(bbId) || this instanceof MutableBagCC;

		workInProgress = true;

		consumed = false;

		shouldLogStart = true;

		chooseOuts(); // This is only needed for MutableBag

		// Treat outputs
		for(Out o: outs) {
			o.startOutBag(outCFLSize);
		}

		// Tell the BagOperator that we are opening a new bag
		int outCFLSizesSize = outCFLSizes.size();
		op.openOutBag();
		if (outCFLSizes.size() < outCFLSizesSize) {
			// This hack is needed here because if openOutBag has already triggered the sending of elements and closing the bag,
			// then we would fail on some later assert. (For example, MutableBag.toBag, fromNothing)
			return;
		}

		for (Input input: inputs) {
			//assert input.finishedSubpartitionCounter == -1;
			assert input.inputCFLSize == -1;

			for (InputSubpartition<IN> sp : input.inputSubpartitions) {
				// Some of these will be set to false by activateLogicalInput soon
				sp.damming = true;
			}
		}

		chooseLogicalInputs(outCFLSize);
	}

	// Note: this activates all the logical inputs. (Cf. the override in PhiNode, which activates only one.)
	protected void chooseLogicalInputs(int outCFLSize) {
		// figure out the input bag IDs
		for (Input input: inputs) {
			int inputCFLSize;
			if (input.inputInSameBlock) {
				inputCFLSize = outCFLSize;
			} else {
				int i;
				for (i = outCFLSize - 2; input.bbId != latestCFL.get(i); i--) {}
				inputCFLSize = i + 1;
			}

			activateLogicalInput(input.id, outCFLSize, inputCFLSize);
		}
	}

	// Note: Also called from PhiNode
	void activateLogicalInput(int id, int outCFLSize, int inputCFLSize) {
		//  - For each subpartition, we tell it what to do:
		//    - Find a buffer that has the appropriate id
		//      - Give all the elements to the BagOperator
		//      - If it is the last one and not finished then remove the dam
		//    - If there is no appropriate buffer, then we do nothing for now
		//  - If we already have a notification that input bag is closed, then we tell this to the operator

		boolean reuse = false;
		if (!inputUses.add(Tuple2.of(id, inputCFLSize))) {
			if (op instanceof ReusingBagOperator && this.reuseInputs) {
				((ReusingBagOperator) op).signalReuse();
				reuse = true;
			}
		}
		op.openInBag(id);

		Input input = inputs.get(id);
		assert input.inputCFLSize == -1;
		input.inputCFLSize = inputCFLSize;
		input.activeFor.add(outCFLSize);
		//assert input.currentBagID == null;
		input.currentBagID = new BagID(input.inputCFLSize, input.opID);
		for(InputSubpartition<IN> sp: input.inputSubpartitions) {
			int i;
			for(i = 0; i < sp.buffers.size(); i++) {
				if(sp.buffers.get(i).bagID.cflSize == input.inputCFLSize)
					break;
			}
			if(i < sp.buffers.size()) { // we have found an appropriate buffer
				assert input.currentBagID.equals(sp.buffers.get(i).bagID);
				if (!reuse) {
					giveBufferToBagOperator(sp, i, id);
				} else {
					consumed = true; // giveBufferToBagOperator would do this, and we still need this event if we are reusing
				}
				if(i == sp.buffers.size() - 1 && sp.status != InputSubpartition.Status.CLOSED) { // the last one and not finished
					sp.damming = false;
					assert !reuse;
				}
			}
		}

		if (notifyCloseInputs.contains(input.currentBagID)) {
			input.closeCurrentInBag();
			if (notifyCloseInputEmpties.contains(input.currentBagID)) {
				consumed = true;
			}
		}

		// I think the "remove buffer if complicated CFG condition" will be needed here
	}

	protected boolean updateOutCFLSizes(List<Integer> cfl) {
		if (cfl.get(cfl.size() - 1).equals(bbId)) {
			outCFLSizes.add(cfl.size());
			return true;
		}
		return false;
	}

	private class MyCFLCallback implements CFLCallback {

		public void notify(List<Integer> cfl) {
			synchronized (es) {
				//List<Integer> cfl = new ArrayList<>(cfl0);
				es.submit(new Runnable() {
					@Override
					public void run() {
						try {
							synchronized (BagOperatorHost.this) {
								latestCFL = cfl;

								if (CFLConfig.vlog) LOG.info("CFL notification: " + latestCFL + " {" + name + "}");

								// Note: the handling of outs has to be before the startOutBag call, because that will throw away buffers
								// (and it often happens that reaching a BB triggers both of these things).

								for (Out o : outs) {
									o.notifyAppendToCFL(cfl);
								}

								//boolean workInProgress = outCFLSizes.size() > 0;
								boolean hasAdded = updateOutCFLSizes(cfl);
								if (!workInProgress && hasAdded) {
									startOutBagCheckBarrier();
								} else {
									if (CFLConfig.vlog)
										LOG.info("[" + name + "] CFLCallback.notify not starting an out bag, because workInProgress=" + workInProgress + ", hasAdded=" + hasAdded + ", outCFLSizes.size()=" + outCFLSizes.size());
								}
							}
						} catch (Throwable t) {
							LOG.error("Unhandled exception in MyCFLCallback.notify: " + ExceptionUtils.stringifyException(t));
							System.err.println(ExceptionUtils.stringifyException(t));
							System.exit(8);
						}
					}
				});
			}
		}

		@Override
		public void notifyTerminalBB() {
			// If this doesn't go through es, then we have the problem that notify submits on subscribe,
			// and this notify would have to insert something into outCFLSizes, but it hasn't inserted it yet
			// when we reach the outCFLSizes check here.
			synchronized (es) {
				es.submit(new Runnable() {
					@Override
					public void run() {
						try {
							LOG.info("CFL notifyTerminalBB {" + name + "}");
							synchronized (BagOperatorHost.this) {
								terminalBBReached = true;
								if (outCFLSizes.isEmpty()) {
									// We have to unsubscribe before shutdown, because we get broadcast notifications
									// even when we are finished, when others are still working.
									cflMan.unsubscribe(cb);
									es.shutdown();
								}
							}
						} catch (Throwable t) {
							LOG.error("Unhandled exception in MyCFLCallback.notifyTerminalBB: " + ExceptionUtils.stringifyException(t));
							System.err.println(ExceptionUtils.stringifyException(t));
							System.exit(8);
						}
					}
				});
			}
		}

		@Override
		public void notifyCloseInput(BagID bagID, int opID) {
			if (opID == BagOperatorHost.this.opID || opID == CFLManager.CloseInputBag.emptyBag) {
				synchronized (es) {
					es.submit(new Runnable() {
						@Override
						public void run() {
							try {
								synchronized (BagOperatorHost.this) {
									assert !notifyCloseInputs.contains(bagID);
									notifyCloseInputs.add(bagID);

									if (opID == CFLManager.CloseInputBag.emptyBag) {
										notifyCloseInputEmpties.add(bagID);
									}

									for (Input inp : inputs) {
										//assert inp.currentBagID != null; // This no longer works, because we broadcast closeInput
										if (bagID.equals(inp.currentBagID)) {

											if (opID == CFLManager.CloseInputBag.emptyBag) {
												// The EmptyFromEmpty marker interface is not needed here, because consumed = true doesn't mess up things
												// even when the result bag will not be empty.
												consumed = true;
											}

											inp.closeCurrentInBag();
										}
									}
								}
							} catch (Throwable t) {
								LOG.error("Unhandled exception in MyCFLCallback.notifyCloseInput: " + ExceptionUtils.stringifyException(t));
								System.err.println(ExceptionUtils.stringifyException(t));
								System.exit(8);
							}
						}
					});
				}
			}
		}

		@Override
		public void notifyBarrierAllReached(int cflSize) {
			synchronized (es) {
				es.submit(new Runnable() {
					@Override
					public void run() {
						synchronized (BagOperatorHost.this) {
							assert CFLManager.barrier;
							barrierAllReachedCFLSize = cflSize;
							if (CFLConfig.vlog)
								LOG.info("notifyBarrierAllReached {" + name + "} cflSize = " + cflSize + ", workInProgress = " + workInProgress);
							if (!workInProgress) {
								if (!outCFLSizes.isEmpty()) {
									if (CFLConfig.vlog)
										LOG.info("notifyBarrierAllReached {" + name + "} calling startOutBagCheckBarrier");
									startOutBagCheckBarrier();
								}
							}
						}
					}
				});
			}
		}

		@Override
		public int getOpID() {
			return BagOperatorHost.this.opID;
		}
	}

	// This overload is for manual job building. The auto builder uses the other one.
	public BagOperatorHost<IN, OUT> out(int splitId, int targetBbId, boolean normal, Partitioner<OUT> partitioner) {
		assert splitId == outs.size();
		outs.add(new Out((byte)splitId, targetBbId, normal, partitioner, Collections.emptySet()));
		return this;
	}

	// `normal` means not conditional.
	// If `normal` is false, then we set to damming until we reach its BB.
	// (This means that for example if targetBbId is the same as the operator's BbId, then we wait for the next iteration step.)
	public int out(int targetBbId, boolean normal, Partitioner<OUT> partitioner, Set<Integer> overwriters) {
		int splitId = outs.size();
		outs.add(new Out((byte)splitId, targetBbId, normal, partitioner, overwriters));
		return splitId;
	}

	private enum OutState {IDLE, DAMMING, WAITING, FORWARDING}

	public final class Out implements Serializable {

		// For the time being, we don't do discard triggered by reaching a BB.
		// (But note that when there is a new out bag, then the old one's buffer is discarded anyway.)

		// There is action in 4 situations:
		//  - startOutBag:
		//    - if it hasn't yet reached target, then DAMMING, and new Buffer
		//    - if it reached target or nornmal, then sendStart and FORWARDING
		//  - element comes from BagOperatorbol
		//    - assert DAMMING or FORWARDING, and then we do the corresponding thing
		//  - end of the bag that is coming from the BagOperator
		//    - IDLE cannot be
		//    - if DAMMING, then we switch to WAITING
		//    - WAITING cannot be
		//    - if FORWARDING, then we check, whether there is a buffer, and if yes, then we send it, and switch to IDLE (there was a D->F switch in this case)
		//  - if the CFL reaches the target
		//    - if IDLE then nothing
		//    - if DAMMING, then sendStart and we switch to FORWARDING
		//    - if WAITING, then we send the buffer, and swtich to IDLE
		//    - if FORWARDING then nothing

		private byte splitId = -1;
		private int targetBbId = -1;
		public boolean normal = false; // not conditional
		public final Partitioner<OUT> partitioner;

		private ArrayList<OUT> buffer = null;
		private OutState state = OutState.IDLE;
		private int outCFLSize = -1; // The CFL that is being emitted. We need this, because cflSizes becomes empty when we become waiting.

		private boolean[] sentStart;

		boolean active;

		private final Set<Integer> overwriters;

		Out(byte splitId, int targetBbId, boolean normal, Partitioner<OUT> partitioner, Set<Integer> overwriters) {
			this.splitId = splitId;
			this.targetBbId = targetBbId;
			this.normal = normal;
			this.partitioner = partitioner;
			this.sentStart = new boolean[partitioner.targetPara];
			this.active = false;
			this.overwriters = overwriters;
		}

		void collectElement(OUT e) {
			if (active) {
				assert state == OutState.FORWARDING || state == OutState.DAMMING;
				if (state == OutState.FORWARDING) {
					sendElement(e);
				} else {
					buffer.add(e);
				}
			}
		}

		void closeBag() {
			if (active) {
				switch (state) {
					case IDLE:
						assert false;
						break;
					case DAMMING:
						state = OutState.WAITING;
						break;
					case WAITING:
						assert false;
						break;
					case FORWARDING:
						if (buffer != null) {
							for (OUT e : buffer) {
								sendElement(e);
							}
						}
						assert outCFLSize == outCFLSizes.peek();
						endBag();
						break;
				}
			}
		}

		void startOutBag(Integer outCFLSize) {
			if (active) {
				boolean targetReached = false;
				if (normal) {
					targetReached = true;
				} else {
					// We don't need +1 for outCFLSize, because it is already the element _after_ outCFL like this
					for (int i = outCFLSize; i < latestCFL.size(); i++) {
						int cfli = latestCFL.get(i);
						if (cfli == targetBbId) {
							targetReached = true;
						}
						if (cfli == bbId || overwriters.contains(cfli)) {
							break; // Because a later bag will override the current one.
						}
					}
				}
				this.outCFLSize = outCFLSize;
				if (!targetReached) {
					state = OutState.DAMMING;
					buffer = new ArrayList<>();
				} else {
					startBag();
					buffer = null; // This is needed because we have some "buffer != null" checks, which would send the old one.
					state = OutState.FORWARDING;
				}
			}
		}

		void notifyAppendToCFL(List<Integer> cfl) {
			// isActive would not be good here, because it happens that a formerly active should be sent only now because of a notify.
			if (!normal && (state == OutState.DAMMING || state == OutState.WAITING)) {
				if (cfl.get(cfl.size() - 1).equals(targetBbId)) {
					// We check that it is not overwritten before reaching the currently added one
					boolean overwritten = false;
					for (int i = outCFLSize; i < cfl.size() - 1; i++) {
						int cfli = cfl.get(i);
						if (cfli == bbId || overwriters.contains(cfli)) {
							overwritten = true;
						}
					}
					if (!overwritten) {
						switch (state) {
							case IDLE:
								assert false; // Because the above if makes sure that this doesn't happen
								break;
							case DAMMING:
								assert outCFLSizes.size() > 0;
								assert outCFLSizes.peek().equals(outCFLSize);
								startBag();
								state = OutState.FORWARDING;
								break;
							case WAITING:
								startBag();
								if (buffer != null) {
									for (OUT e : buffer) {
										sendElement(e);
									}
								}
								endBag();
								state = OutState.IDLE;
								break;
							case FORWARDING:
								assert false; // Because the above if makes sure that this doesn't happen
								break;
						}
					}
				}
			}
		}



		private StreamRecord<OUT> reuseStreamRecord = new StreamRecord<>(null);
		private ElementOrEvent<OUT> reuseEleOrEvent = new ElementOrEvent<>();

		void sendElement(OUT e) {
			short part = partitioner.getPart(e, subpartitionId);
			if (part != -1) {
				// Note that this logic is duplicated on Bagify, but without the -1 (broadcast) handling
				if (!sentStart[part]) {
					sendStart(part);
				}
			} else {
				broadcastStart();
			}
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending element to " + part + ": " + new ElementOrEvent<>(subpartitionId, e, splitId, part));

			//output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, e, splitId, part), 0));
			output.collect(reuseStreamRecord.replace(reuseEleOrEvent.replace(subpartitionId, e, splitId, part)));
		}

		void broadcastStart() {
			for (short i=0; i<sentStart.length; i++) {
				sendStart(i);
			}
		}

		void startBag() {
			for (int i=0; i<sentStart.length; i++)
				sentStart[i] = false;
		}

		void endBag() {
			for (short i=0; i<sentStart.length; i++) {
				if (sentStart[i]) {
					sendEnd(i);
				}
			}

			buffer = null;
		}

		private void sendStart(short part) {
			sentStart[part] = true;
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.START, partitioner.targetPara, new BagID(outCFLSize, opID));
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending START to " + part + ": " + new ElementOrEvent<>(subpartitionId, event, splitId, part));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part)));
		}

		private void sendEnd(short part) {
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending END to " + part);
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, partitioner.targetPara, new BagID(outCFLSize, opID));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part)));
		}
	}

	// Logical input
	final class Input implements Serializable {

		int id; // index in the inputs array
		int bbId;
		boolean inputInSameBlock; // before in the code (so if it is after in the same block, then this is false)
		InputSubpartition<IN>[] inputSubpartitions;
		int inputCFLSize = -1; // always -1 when not working on an output bag or when we are not taking part in the computation of the current out bag (PhiNode)
		BagID currentBagID = null; // which we are workign from
		int opID = -1;
		Set<Integer> activeFor = new HashSet<>(); // outCFLSizes for which this Input is active

		Input(int id, int bbId, boolean inputInSameBlock, int opID) {
			this.id = id;
			this.bbId = bbId;
			this.inputInSameBlock = inputInSameBlock;
			this.opID = opID;
		}

		void closeCurrentInBag() {
			inputCFLSize = -1;
			//currentBagID = null; // This would be bad here, because we need it in the below call
			op.closeInBag(id);
		}
	}

	private final static class InputSubpartition<T> {

		enum Status {OPEN, CLOSED}

		class Buffer {
			SerializedBuffer<T> elements;
			final BagID bagID;

			Buffer(BagID bagID) {
				this.elements = new SerializedBuffer<T>(ser);
				this.bagID = bagID;
			}
		}

		final ArrayList<Buffer> buffers;

		Status status;

		boolean damming;

		final TypeSerializer<T> ser;

		private final boolean throwAwayOldBufs;
		private int thrown = 0; // has already thrown away all input bufs before this

		InputSubpartition(TypeSerializer<T> ser, boolean throwAwayOldBufs) {
			this.ser = ser;
			this.throwAwayOldBufs = throwAwayOldBufs;
			buffers = new ArrayList<>();
			status = Status.CLOSED;
			damming = false;
		}

		void addNewBuffer(BagID bagID) {

			if (throwAwayOldBufs) {
				// Old buffers are hopefully not needed. (Ideally, we would actually check some complicated condition on the control flow graph.)
				// Setting elements to null instead of throwing the buffer away ensures that we easily notice if this assumption is violated.
				// Note: We set up the conditional outputs in such a way that we never send a buffer that won't be ever be used,
				// so the consumeStarted condition shouldn't pin stuff forever.
				boolean allNullSoFar = true;
				for (int i = thrown; i < buffers.size() - 2; i++) {
					Buffer bufI = buffers.get(i);
					if (bufI.elements != null && bufI.elements.consumeStarted) { // throw away only if it was already used
						bufI.elements = null;
					}

					if (allNullSoFar && bufI.elements != null) {
						allNullSoFar = false;
						thrown = i;
					}
				}
				if (allNullSoFar) {
					thrown = Math.max(0, buffers.size() - 2);
				}
			}

			buffers.add(new Buffer(bagID));
		}
	}
}
