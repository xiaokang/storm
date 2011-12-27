package backtype.storm.dedup;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class DedupBoltContext implements IDedupContext {
  
  private static final Log LOG = LogFactory.getLog(DedupBoltContext.class);
  
  private Map<String, String> conf;
  private TopologyContext context;
  private OutputCollector collector;
  
  private OutputFieldsDeclarer declarer;
  
  private Tuple currentInput;
  private String currentInputID;
  private int currentOutputIndex;
  
  /**
   * key-value state
   */
  private IStateStore stateStore;
  private byte[] storeKey;
  
  private Map<byte[], byte[]> stateMap;
  private Map<byte[], byte[]> newState;
  
  private class Output {
    public String streamId;
    public List<Object> tuple;
    
    public Output(String streamId, List<Object> tuple) {
      this.streamId = streamId;
      this.tuple = tuple;
    }
    
    public byte[] getBytes() {
      return null; // TODO
    }
    
    public void fromString(String str) {
      
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
    
    this.stateStore = new HBaseStateStore(context.getStormId());
    stateStore.open();
    this.storeKey = Bytes.toBytes(context.getThisComponentId());
    
    this.stateMap = new HashMap<byte[], byte[]>();
    this.newState = new HashMap<byte[], byte[]>();
    this.outputMap = new HashMap<String, Output>();
    this.newOutput = new HashMap<String, Output>();
  }
  
  
  /**
   * DedupSpoutContext specific method
   */
  
  public void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
    this.declarer = declarer;
  }
  
  public void execute(IDedupBolt bolt, Tuple input) throws IOException {
    this.currentInput = input;
    this.currentOutputIndex = 0;
    this.newState.clear();
    this.newOutput.clear();
    
    List<Object> list = input.select(new Fields(DedupConstants.TUPLE_ID_FIELD, 
        DedupConstants.TUPLE_TYPE_FIELD));
    this.currentInputID = (String)list.get(0);
    DedupConstants.TUPLE_TYPE type = 
      DedupConstants.TUPLE_TYPE.valueOf((String)list.get(1));
    if (DedupConstants.TUPLE_TYPE.NORMAL == type) {
      // call user bolt
      bolt.execute(this, input);
      
      // persistent user bolt set state and output tuple
      if (newState.size() > 0 || newOutput.size() > 0) {
        Map<byte[], Map<byte[], byte[]>> updateMap = 
          new HashMap<byte[], Map<byte[], byte[]>>();
        if (newState.size() > 0) {
          updateMap.put(Bytes.toBytes(IStateStore.STATEMAP), newState);
        }
        if (newOutput.size() > 0) {
          Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
          for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
            map.put(Bytes.toBytes(entry.getKey()), 
                entry.getValue().getBytes());
          }
          updateMap.put(Bytes.toBytes(IStateStore.OUTPUTMAP), map);
        }
        stateStore.set(storeKey, updateMap);
        updateMap.clear();
      }
      
      // really emit output
      for (Map.Entry<String, Output> entry : newOutput.entrySet()) {
        collector.emit(entry.getValue().streamId, input, 
            entry.getValue().tuple);
      }
    } else if (DedupConstants.TUPLE_TYPE.DUPLICATE == type) {
      String prefix = currentInputID + DedupConstants.TUPLE_ID_SEP;
      for (String tupleid : outputMap.keySet()) {
        if (tupleid.startsWith(prefix)) {
          // just re-send output
          Output output = outputMap.get(tupleid);
          collector.emit(output.streamId, input, output.tuple);
        }
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
              entry.getValue().getBytes());
          // remove from memory
          it.remove();
        }
      }
      // remove from persistent store
      if (deleteMap.size() > 0) {
        Map<byte[], Map<byte[], byte[]>> delete = 
          new HashMap<byte[], Map<byte[],byte[]>>();
        delete.put(Bytes.toBytes(IStateStore.OUTPUTMAP), deleteMap);
        stateStore.delete(storeKey, delete);
      }
    } else {
      LOG.warn("unknown type " + type);
    }

    // ack the input tuple
    collector.ack(input);
    
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
    String tupleid = currentInputID + DedupConstants.TUPLE_ID_SEP + 
      context.getThisComponentId() + 
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
  }

  @Override
  public String getConf(String confName) {
    return conf.get(confName);
  }
  
  @Override
  public byte[] getConf(byte[] key) {
    try {
      return conf.get(new String(key, "UTF8")).getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  @Override
  public byte[] getState(byte[] key) {
    return stateMap.get(key);
  }

  @Override
  public boolean setState(byte[] key, byte[] value) {
    newState.put(key, value);
    stateMap.put(key, value);
    return true;
  }

  
  /**
   * implement OutputFieldsDeclarer method
   */
  
  @Override
  public void declare(Fields fields) {
    // add tow fields to original fields
    List<String> fieldList = fields.toList();
    fieldList.add(DedupConstants.TUPLE_ID_FIELD);
    fieldList.add(DedupConstants.TUPLE_TYPE_FIELD);
    declarer.declare(new Fields(fieldList));
  }

  @Override
  public void declare(boolean direct, Fields fields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void declareStream(String streamId, Fields fields) {
    // TODO Auto-generated method stub

  }

  @Override
  public void declareStream(String streamId, boolean direct, Fields fields) {
    // TODO Auto-generated method stub

  }

}
