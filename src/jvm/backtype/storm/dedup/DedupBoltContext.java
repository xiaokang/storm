package backtype.storm.dedup;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class DedupBoltContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupBoltContext.class);
  
  private Map<String, String> conf;
  private TopologyContext context;
  private OutputCollector collector;
  
  private String uniqID;
  
  private Tuple currentInput;
  private String currentInputID;
  private int currentOutputIndex;
  
  /**
   * key-value state
   */
  private IStateStore stateStore;
  private byte[] storeKey;
  
  private Map<BytesArrayRef, byte[]> stateMap;
  private Map<BytesArrayRef, byte[]> newState;
  
  private static class Output implements Serializable {
    public String streamId;
    public List<Object> tuple;
    
    public Output(String streamId, List<Object> tuple) {
      this.streamId = streamId;
      this.tuple = tuple;
    }
  }
  /**
   * message id => tuple
   */
  private Map<String, Output> outputMap;
  private Map<String, Output> newOutput;
  
  public DedupBoltContext(Map stormConf, TopologyContext context,
      OutputCollector collector) throws IOException {
    this.conf = stormConf;
    this.context = context;
    this.collector = collector;
    
    this.uniqID = context.getThisComponentId() + ":" + context.getThisTaskId();

    this.stateMap = new HashMap<BytesArrayRef, byte[]>();
    this.newState = new HashMap<BytesArrayRef, byte[]>();
    this.outputMap = new HashMap<String, Output>();
    this.newOutput = new HashMap<String, Output>();

    this.storeKey = Bytes.toBytes(uniqID);
    
    this.stateStore = new HBaseStateStore(context.getStormId());
    stateStore.open();
    // read saved data
    Map<byte[], Map<byte[], byte[]>> storeMap = stateStore.get(storeKey);
    if (storeMap != null) {
      for (Entry<byte[], Map<byte[], byte[]>> entry : storeMap.entrySet()) {
        if (Arrays.equals(IStateStore.STATEMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> stateEntry : entry.getValue().entrySet()) {
            stateMap.put(new BytesArrayRef(stateEntry.getKey()), 
                stateEntry.getValue());
            LOG.info(uniqID + " load STATEMAP");
          }
        } else if (Arrays.equals(IStateStore.OUTPUTMAP, entry.getKey())) {
          for (Entry<byte[], byte[]> outEntry : entry.getValue().entrySet()) {
            outputMap.put(Bytes.toString(outEntry.getKey()), 
                (Output)Utils.deserialize(outEntry.getValue()));
            LOG.info(uniqID + " load OUTPUTMAP");
          }
        } else {
          LOG.warn("unknown state " + Bytes.toString(entry.getKey()));
        }
      }
    }
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void execute(IDedupBolt bolt, Tuple input) throws IOException {
    this.currentInput = input;
    this.currentOutputIndex = 0;
    
    this.currentInputID = input.getStringByField(DedupConstants.TUPLE_ID_FIELD);
    DedupConstants.TUPLE_TYPE type = 
      DedupConstants.TUPLE_TYPE.valueOf(
          input.getStringByField(DedupConstants.TUPLE_TYPE_FIELD));
    LOG.info(uniqID + " receive " + type + " input tuple " + currentInputID);

    boolean needProcess = true;
    if (DedupConstants.TUPLE_TYPE.DUPLICATE == type || 
        DedupConstants.TUPLE_TYPE.NORMAL == type) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      for (String tupleid : outputMap.keySet()) {
        if (tupleid.startsWith(prefix)) {
          // just re-send output
          Output output = outputMap.get(tupleid);
          collector.emit(output.streamId, input, output.tuple);
          LOG.warn(uniqID + " re-emit output for input tuple " + 
              currentInputID + " to " + output.streamId);
          needProcess = false;
        }
      }
    }
    
    if (DedupConstants.TUPLE_TYPE.DUPLICATE == type || 
        DedupConstants.TUPLE_TYPE.NORMAL == type && needProcess) {
      // call user bolt
      bolt.execute(this, input);
      
      // persistent user bolt set state and output tuple
      if (newState.size() > 0 || newOutput.size() > 0) {
        Map<byte[], Map<byte[], byte[]>> updateMap = 
          new HashMap<byte[], Map<byte[], byte[]>>();
        if (newState.size() > 0) {
          Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
          for (Map.Entry<BytesArrayRef, byte[]> entry : newState.entrySet()) {
            map.put(entry.getKey().getBytes(), entry.getValue());
          }
          updateMap.put(IStateStore.STATEMAP, map);
        }
        if (newOutput.size() > 0) {
          Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
          for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
            map.put(Bytes.toBytes(entry.getKey()), 
                Utils.serialize(entry.getValue()));
          }
          updateMap.put(IStateStore.OUTPUTMAP, map);
        }
        stateStore.set(storeKey, updateMap);
        LOG.info(uniqID + " store " + currentInputID);
        updateMap.clear();
      }
      
      // really emit output
      for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
        collector.emit(entry.getValue().streamId, input, 
            entry.getValue().tuple);
        LOG.info(uniqID + " real emit tuple " + currentInputID + " to " + 
            entry.getValue().streamId);
      }
    } else if (DedupConstants.TUPLE_TYPE.NOTICE == type) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      Map<byte[], byte[]> deleteMap = new HashMap<byte[], byte[]>();
      Iterator<Map.Entry<String, Output>> it = outputMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, Output> entry = it.next();
        if (entry.getKey().startsWith(prefix)) {
          // add to delete map
          deleteMap.put(Bytes.toBytes(entry.getKey()), 
              Utils.serialize(entry.getValue()));
          // remove from memory
          it.remove();
        }
      }
      // remove from persistent store
      if (deleteMap.size() > 0) {
        Map<byte[], Map<byte[], byte[]>> delete = 
          new HashMap<byte[], Map<byte[],byte[]>>();
        delete.put(IStateStore.OUTPUTMAP, deleteMap);
        stateStore.delete(storeKey, delete);
        LOG.info(uniqID + " delete output for input tuple " + currentInputID);
      }
    } else {
      LOG.warn(uniqID + " unknown type " + type);
    }

    // ack the input tuple
    collector.ack(input);
    LOG.info(uniqID + " ack input tuple " + currentInputID);
    
    this.currentInput = null;
    this.newState.clear();
    this.newOutput.clear();
  }
  
  
  /**
   * implement IDedupContext method
   */

  @Override
  public void emit(List<Object> tuple) {
    emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  @Override
  public void emit(String streamId, List<Object> tuple) {
    // tupleid : component1-outindex_component2-outindex
    String tupleid = currentInputID + DedupConstants.TUPLE_ID_SEP + 
      uniqID + 
      DedupConstants.TUPLE_ID_SUB_SEP + 
      currentOutputIndex;
    currentOutputIndex++;
    // add tuple id to tuple
    tuple.add(tupleid.toString());
    // add tuple type to tuple
    tuple.add(DedupConstants.TUPLE_TYPE.NORMAL.toString());
    
    // add tuple to output buffer
    Output output = new Output(streamId, tuple);
    newOutput.put(tupleid, output);
    outputMap.put(tupleid, output);
    LOG.info(uniqID + " user emit " + tupleid + " to " + streamId);
  }

  @Override
  public String getConf(String confName) {
    return conf.get(confName);
  }
  
  @Override
  public byte[] getConf(byte[] key) {
    return Bytes.toBytes(conf.get(Bytes.toString(key)));
  }

  @Override
  public byte[] getState(byte[] key) {
    return stateMap.get(new BytesArrayRef(key));
  }

  @Override
  public boolean setState(byte[] key, byte[] value) {
    newState.put(new BytesArrayRef(key), value);
    stateMap.put(new BytesArrayRef(key), value);
    LOG.info(uniqID + " set state " + 
        Bytes.toString(key) + " : " + Bytes.toString(value));
    return true;
  }
}
