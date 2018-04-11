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
	private TypeSerializer<IN> inSer;

	// ---------------------- Initialized in setup (i.e., on TM):

	public short subpartitionId = -25;

	private CFLManager cflMan;
	private MyCFLCallback cb;

	protected ArrayList<Input> inputs;

	// ----------------------

	protected List<Integer> latestCFL; //majd vigyazni, hogy ez valszeg ugyanaz az objektumpeldany, mint ami a CFLManagerben van
	protected Queue<Integer> outCFLSizes; // ha nem ures, akkor epp az elson dolgozunk; ha ures, akkor nem dolgozunk

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

		cflMan = CFLManager.getSing();

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
		assert eleOrEvent.logicalInputId != -1; // (kell egy extra map, ami kitolti (ha nem egyinputos))
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
					assert eleOrEvent.event.assumedTargetPara == getRuntimeContext().getNumberOfParallelSubtasks();
					assert sp.status == InputSubpartition.Status.CLOSED;
					sp.status = InputSubpartition.Status.OPEN;
					sp.addNewBuffer(ev.bagID);
					assert input.opID == ev.bagID.opID;
					//assert input.currentBagID == null || input.currentBagID.equals(ev.bagID); // ezt azert kellett kicommentezni, mert a null-ra allitast kivettem, mert rossz helyen volt
					//assert input.currentBagID.equals(ev.bagID); // Ez meg azert nem igaz, mert van, hogy az activateLogicalInput meg nem fut le, amikor a start mar megerkezik
					// A kov ertekadas azert nem jo, mert igy a notifyCloseInput azt hinne, hogy mar aktivalva lett.
					// De lehet, hogy csak kesobb lesz aktivalva (es akkor majd persze megnezzuk, hogy kaptunk-e mar notifyCloseInput-ot ra).
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
					assert eleOrEvent.event.assumedTargetPara == getRuntimeContext().getNumberOfParallelSubtasks();
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
			assert outs.size() != 0; // If the operator wants to emit an element but there are no outs, then we just probably forgot to call .out()
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
				// a kov. assert akkor mondjuk elromolhat, ha ilyen short-circuit-es jellegu az operator, hogy van, hogy mar akkor
				// lezarja az output bag-et, amikor meg nem kezdodott el minden inputon az input bag, de kesobb meg fog onnan jonni.
				//assert inp.currentBagID != null; // PhiNode-nal nem fasza
				if (inp.activeFor.contains(outCFLSizes.peek())) {
					assert inp.currentBagID != null;
					inputBagIDs.add(inp.currentBagID);
				}

				// Ez megint csak elvileg elromolhat olyan short-circuit-es operatornal, ami nem varja meg, hogy minden inputja lezarodjon, mielott lezarna az out-ot.
				assert inp.inputCFLSize == -1;
			}
			BagID[] inputBagIDsArr = new BagID[inputBagIDs.size()];
			int i = 0;
			for (BagID b: inputBagIDs) {
				inputBagIDsArr[i++] = b;
			}

			BagID outBagID = new BagID(outCFLSizes.peek(), opID);
			if (numElements > 0 || consumed || inputBagIDs.size() == 0) {
				// inputBagIDs.size() == 0 esetben azert kell kuldenunk, mert ilyenkor a checkForClosingProduced mindenhonnan var
				// (az elejen levo (s.inputs.size() == 0) if miatt)
				if (!(BagOperatorHost.this instanceof MutableBagCC && ((MutableBagCC.MutableBagOperator)op).inpID == 2)) {
					numElements = correctBroadcast(numElements);
					cflMan.producedLocal(outBagID, inputBagIDsArr, numElements, getRuntimeContext().getNumberOfParallelSubtasks(), subpartitionId, opID);
				}
			}

			numElements = 0;

			outCFLSizesRemove();
			workInProgress = false;
			if(outCFLSizes.size() > 0) { // ha van jelenleg varakozo munka
				if (CFLConfig.vlog) LOG.info("Out.closeBag starting a new out bag {" + name + "}");
				// Note: ettol el fog dobodni az Outok buffere, de ez nem baj, mert aminek el kellett mennie az mar elment
				startOutBagCheckBarrier();
			} else {
				if (CFLConfig.vlog) LOG.info("Out.closeBag not starting a new out bag {" + name + "}");
				if (terminalBBReached) { // ha nincs jelenleg varakozo munka es mar nem is jon tobb
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
					// most van egy ilyen limitation-unk, hogy ha egy output broadcast output, akkor az osszesnek annak kell lennie
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

		chooseOuts(); // Ez csak a MutableBag-nel kell

		// Treat outputs
		for(Out o: outs) {
			o.startOutBag(outCFLSize);
		}

		// Tell the BagOperator that we are opening a new bag
		int outCFLSizesSize = outCFLSizes.size();
		op.openOutBag();
		if (outCFLSizes.size() < outCFLSizesSize) {
			// Ez a mokolas itt azert kell, mert ha mar esetleg az openOutBag kivaltotta az elemek kikuldeset es a bag lezarasat, akkor a
			// kesobbiekben elszallnank vmi asserten. (Ez a MutableBag.toBag-nel tortenik pl.)
			return;
		}

		for (Input input: inputs) {
			//assert input.finishedSubpartitionCounter == -1;
			assert input.inputCFLSize == -1;

			for (InputSubpartition<IN> sp : input.inputSubpartitions) {
				sp.damming = true; // ezek kozul ugyebar nemelyiket majd false-ra allitja az activateLogicalInput mindjart
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

		// Asszem itt kell majd a remove buffer if bonyolult CFG-s condition
	}

	protected boolean updateOutCFLSizes(List<Integer> cfl) {
		if (cfl.get(cfl.size() - 1).equals(bbId)) {
			outCFLSizes.add(cfl.size());
			return true;
		}
		return false;
	}

	private class MyCFLCallback implements CFLCallback {

		public void notify(List<Integer> cfl0) {
			synchronized (es) {
				List<Integer> cfl = new ArrayList<>(cfl0);
				es.submit(new Runnable() {
					@Override
					public void run() {
						try {
							synchronized (BagOperatorHost.this) {
								latestCFL = cfl;

								if (CFLConfig.vlog) LOG.info("CFL notification: " + latestCFL + " {" + name + "}");

								// Note: figyelni kell, hogy itt hamarabb legyen az out-ok kezelese, mint a startOutBag hivas, mert az el fogja dobni a buffereket,
								// es van olyan, hogy ugyanannak a BB-nek az elerese mindket dolgot kivaltja

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
			// ha ez nem az es-en keresztul megy, akkor az a gond, hogy subscribe-nal a notify submittel,
			// es ennek a notify-nak kene beraknia vmit az outCFLSizes-ba, de meg nem rakta be, amikor mar elerjuk a itt az outCFLSizes checket
			synchronized (es) {
				es.submit(new Runnable() {
					@Override
					public void run() {
						try {
							LOG.info("CFL notifyTerminalBB {" + name + "}");
							synchronized (BagOperatorHost.this) {
								terminalBBReached = true;
								if (outCFLSizes.isEmpty()) {
									// azert kell elobb unsubscribe-olni, mint shutdown-olni, mert a olyankor is kapunk mindenfele
									// broadcast jellegu notificationoket, amikor mi mar vegeztunk, de a tobbiek meg dolgoznak.
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
										//assert inp.currentBagID != null; // Ez kozben megsem lesz igaz, mert mostmar broadcastoljuk a closeInput-ot
										if (bagID.equals(inp.currentBagID)) {

											if (opID == CFLManager.CloseInputBag.emptyBag) {
												// Itt az EmptyFromEmpty marker interface azert nem kell, mert nem rontja ez el a dolgokat a consumed = true
												// akkor sem, ha nem empty lesz az eredmeny bag.
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

		// Egyelore nem csinalunk kulon BB elerese altal kivaltott discardot. (Amugy ha uj out bag van, akkor eldobodik a reginek a buffere igy is.)

		// 4 esetben tortenik valami:
		//  - startOutBag:
		//    - ha nem erte el a targetet, akkor DAMMING, es new Buffer
		//    - ha elerte a targetet vagy normal, akkor sendStart es FORWARDING
		//  - elem jon a BagOperatorbol
		//    - assert DAMMING or FORWARDING, es aztan tesszuk a megfelelot
		//  - vege van a BagOperatorbol jovo bagnek
		//    - IDLE nem lehet
		//    - ha DAMMING, akkor WAITING-re valtunk
		//    - WAITING nem lehet
		//    - ha FORWARDING, akkor megnezzuk, hogy van-e buffer, es ha igen, akkor kuldjuk, es IDLE-re valtunk (ugye ekkor kozben volt D->F valtas)
		//  - a CFL eleri a targetet
		//    - IDLE akkor semmi
		//    - ha DAMMING, akkor sendStart es FORWARDING-ra valtunk
		//    - ha WAITING, akkor elkuldjuk a buffert, es IDLE-re valtunk
		//    - FORWARDING akkor semmi

		private byte splitId = -1;
		private int targetBbId = -1;
		public boolean normal = false; // jelzi ha nem conditional
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
					// Azert nem kell +1 az outCFLSize-nak, mert ugye az outCFL _utani_ elem igy is
					for (int i = outCFLSize; i < latestCFL.size(); i++) {
						int cfli = latestCFL.get(i);
						if (cfli == targetBbId) {
							targetReached = true;
						}
						if (cfli == bbId || overwriters.contains(cfli)) {
							break; // Merthogy akkor egy kesobbi bag folul fogja irni a mostanit.
						}
					}
				}
				this.outCFLSize = outCFLSize;
				if (!targetReached) {
					state = OutState.DAMMING;
					buffer = new ArrayList<>();
				} else {
					startBag();
					buffer = null; // Ez azert kell, mert vannak ilyen buffer != null checkek, es azok elkuldenek a regit
					state = OutState.FORWARDING;
				}
			}
		}

		void notifyAppendToCFL(List<Integer> cfl) {
			// Itt azert nem jo az isActive, mert van, hogy egy korabban aktivat kene meg csak most elkuldeni a notify hatasara.
			if (!normal && (state == OutState.DAMMING || state == OutState.WAITING)) {
				if (cfl.get(cfl.size() - 1).equals(targetBbId)) {
					// Leellenorizzuk, hogy nem irodik-e felul, mielott meg a jelenleg hozzaadottat elerne
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
								assert false; // Csak azert, mert a fentebbi if kizarja
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
								assert false; // Csak azert, mert a fentebbi if kizarja
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
				// (Amugy ez a logika meg van duplazva a Bagify-ban is most, de ott nincs berakva a -1 (broadcast) kezelese)
				if (!sentStart[part]) {
					sendStart(part);
				}
			} else {
				broadcastStart();
			}
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending element to " + part + ": " + new ElementOrEvent<>(subpartitionId, e, splitId, part));

			//output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, e, splitId, part), 0));
			output.collect(reuseStreamRecord.replace(reuseEleOrEvent.replace(subpartitionId, e, splitId, part), 0));
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
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part), 0));
		}

		private void sendEnd(short part) {
			if (CFLConfig.vlog) LOG.info("Out("+ splitId + ") of {" + name + "}[" + BagOperatorHost.this.subpartitionId + "] sending END to " + part);
			ElementOrEvent.Event event = new ElementOrEvent.Event(ElementOrEvent.Event.Type.END, partitioner.targetPara, new BagID(outCFLSize, opID));
			output.collect(new StreamRecord<>(new ElementOrEvent<>(subpartitionId, event, splitId, part), 0));
		}
	}

	// Logical input
	final class Input implements Serializable {

		int id; // marmint sorszam az inputs tombben
		int bbId;
		boolean inputInSameBlock; // marmint ugy ertve, hogy a kodban elotte (szoval ha utana van ugyanabban a blockban, akkor ez false)
		InputSubpartition<IN>[] inputSubpartitions;
		int inputCFLSize = -1; // always -1 when not working on an output bag or when we are not taking part in the computation of the current out bag (PhiNode)
		BagID currentBagID = null; // marmint ugy current, hogy amibol epp dolgozunk
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
			//currentBagID = null; // ez itt rossz lenne, mert meg szuksegunk van ra kov hivasbol jovo dolgoknal
			op.closeInBag(id);
		}
	}

	private final static class InputSubpartition<T> {

		enum Status {OPEN, CLOSED}

		class Buffer {
			SerializedBuffer<T> elements;
			BagID bagID;

			Buffer(BagID bagID) {
				this.elements = new SerializedBuffer<T>(ser);
				this.bagID = bagID;
			}
		}

		ArrayList<Buffer> buffers;

		Status status;

		boolean damming;

		TypeSerializer<T> ser;

		private final boolean throwAwayOldBufs;

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
				for (int i = 0; i < buffers.size() - 2; i++) {
					Buffer bufI = buffers.get(i);
					if (bufI.elements != null && bufI.elements.consumeStarted) { // throw away only if it was already used
						bufI.elements = null;
					}
				}
			}

			buffers.add(new Buffer(bagID));
		}
	}
}
