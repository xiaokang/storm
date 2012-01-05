package backtype.storm.dedup;

public interface DedupConstants {
  /**
   * dedup message stream
   */
  public static final String DEDUP_STREAM_ID = "_DEDUP_STREAM_ID_";
  
  /**
   * tuple id field
   */
  public static final String TUPLE_ID_FIELD = "_TUPLE_ID_";
  
  /**
   * tuple type field
   */
  public static final String TUPLE_TYPE_FIELD = "_TUPLE_TYPE_";
  /**
   * tuple type enum
   */
  public enum TUPLE_TYPE {NORMAL, DUPLICATE, NOTICE};
  
  public static final String TUPLE_ID_SEP = "_";
  public static final String TUPLE_ID_SUB_SEP = "-";
}