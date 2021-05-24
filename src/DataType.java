package dbProject;

import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.time.*;

public enum DataType {
  NULL((byte) 0), TINYINT((byte) 1), SMALLINT((byte) 2), INT((byte) 3), BIGINT((byte) 4), FLOAT((byte) 5),
  DOUBLE((byte) 6), YEAR((byte) 8), TIME((byte) 9), DATETIME((byte) 10), DATE((byte) 11), TEXT((byte) 12);

  @Override
  public String toString() {
    switch (this) {
      case NULL:
        return "NULL";
      case TINYINT:
        return "TINYINT";
      case SMALLINT:
        return "SMALLINT";
      case INT:
        return "INT";
      case BIGINT:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case YEAR:
        return "YEAR";
      case TIME:
        return "TIME";
      case DATETIME:
        return "DATETIME";
      case DATE:
        return "DATE";
      case TEXT:
        return "TEXT";

      default:
        throw new IllegalArgumentException();
    }
  }

  private static final Map<Byte, DataType> dataTypeLookup = new HashMap<Byte, DataType>();
  private static final Map<Byte, Integer> dataTypeSizeLookup = new HashMap<Byte, Integer>();
  private static final Map<String, DataType> dataTypeStringLookup = new HashMap<String, DataType>();
  private static final Map<DataType, Integer> dataTypePrintOffset = new HashMap<DataType, Integer>();

  static {
    for (DataType s : DataType.values()) {
      dataTypeLookup.put(s.getValue(), s);
      dataTypeStringLookup.put(s.toString(), s);

      if (s == DataType.TINYINT || s == DataType.YEAR) {
        dataTypeSizeLookup.put(s.getValue(), 1);
        dataTypePrintOffset.put(s, 6);
      } else if (s == DataType.SMALLINT) {
        dataTypeSizeLookup.put(s.getValue(), 2);
        dataTypePrintOffset.put(s, 8);
      } else if (s == DataType.INT || s == DataType.FLOAT || s == DataType.TIME) {
        dataTypeSizeLookup.put(s.getValue(), 4);
        dataTypePrintOffset.put(s, 10);
      } else if (s == DataType.BIGINT || s == DataType.DOUBLE || s == DataType.DATETIME || s == DataType.DATE) {
        dataTypeSizeLookup.put(s.getValue(), 8);
        dataTypePrintOffset.put(s, 25);
      } else if (s == DataType.TEXT) {
        dataTypePrintOffset.put(s, 25);
      } else if (s == DataType.NULL) {
        dataTypeSizeLookup.put(s.getValue(), 0);
        dataTypePrintOffset.put(s, 6);
      }
    }

  }

  private byte value;

  private DataType(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  public static DataType get(byte value) {
    if (value > 12)
      return DataType.TEXT;
    return dataTypeLookup.get(value);
  }

  public static DataType get(String text) {
    return dataTypeStringLookup.get(text);
  }

  public static int getLength(DataType type) {
    return getLength(type.getValue());
  }

  public static int getLength(byte value) {
    if (get(value) != DataType.TEXT)
      return dataTypeSizeLookup.get(value);
    else
      return value - 12;
  }

  public int getPrintOffset() {
    return dataTypePrintOffset.get(get(this.value));
  }

  public int compare(String value, String compareValue) {
    switch (this) {
      case NULL:
        return 0;
      case TINYINT:
        return Byte.valueOf(value).compareTo(Byte.valueOf(compareValue));
      case SMALLINT:
        return Short.valueOf(value).compareTo(Short.valueOf(compareValue));
      case INT:
        return Integer.valueOf(value).compareTo(Integer.valueOf(compareValue));
      case BIGINT:
        return Long.valueOf(value).compareTo(Long.valueOf(compareValue));
      case FLOAT:
        return Float.valueOf(value).compareTo(Float.valueOf(compareValue));
      case DOUBLE:
        return Double.valueOf(value).compareTo(Double.valueOf(compareValue));
      case YEAR:
        return Integer.valueOf(value).compareTo(Integer.valueOf(compareValue));
      case TIME:
        return LocalTime.parse(value).compareTo(LocalTime.parse(compareValue));
      case DATETIME:
        return LocalDateTime.parse(value).compareTo(LocalDateTime.parse(compareValue));
      case DATE:
        return LocalDate.parse(value).compareTo(LocalDate.parse(compareValue));
      case TEXT:
        return String.valueOf(value).compareTo(String.valueOf(compareValue));
      default:
        throw new IllegalArgumentException();
    }
  }

  public int compare(Attribute value, Attribute compareValue) {
    if (this == NULL) {
      return 0;
    } else if (this == TINYINT) {
      byte left = Byte.valueOf(ByteBuffer.wrap(value.fieldValuebyte).get());
      byte right = Byte.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).get());
      return Byte.valueOf(left).compareTo(Byte.valueOf(right));
    } else if (this == SMALLINT) {
      short left = Short.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getShort());
      short right = Short.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getShort());
      return Short.valueOf(left).compareTo(Short.valueOf(right));
    } else if (this == INT) {
      int left = Integer.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getInt());
      int right = Integer.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getInt());
      return Integer.valueOf(left).compareTo(Integer.valueOf(right));
    } else if (this == BIGINT) {
      Long left = Long.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getLong());
      Long right = Long.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getLong());
      return Long.valueOf(left).compareTo(Long.valueOf(right));
    } else if (this == FLOAT) {
      Float left = Float.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getFloat());
      Float right = Float.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getFloat());
      return Float.valueOf(left).compareTo(Float.valueOf(right));
    } else if (this == DOUBLE) {
      Double left = Double.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getDouble());
      Double right = Double.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getDouble());
      return Double.valueOf(left).compareTo(Double.valueOf(right));
    } else if (this == YEAR) {
      int left = Integer.valueOf((int) Byte.valueOf(ByteBuffer.wrap(value.fieldValuebyte).get()) + 2000);
      int right = Integer.valueOf((int) Byte.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).get()) + 2000);
      return Integer.valueOf(left).compareTo(Integer.valueOf(right));
    } else if (this == TIME) {
      int left = ByteBuffer.wrap(value.fieldValuebyte).getInt();
      int right = ByteBuffer.wrap(compareValue.fieldValuebyte).getInt();
      return Integer.valueOf(left).compareTo(Integer.valueOf(right));
    } else if (this == DATETIME) {
      Long left = Long.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getLong());
      Long right = Long.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getLong());
      return Long.valueOf(left).compareTo(Long.valueOf(right));
    } else if (this == DATE) {
      Long left = Long.valueOf(ByteBuffer.wrap(value.fieldValuebyte).getLong());
      Long right = Long.valueOf(ByteBuffer.wrap(compareValue.fieldValuebyte).getLong());
      return Long.valueOf(left).compareTo(Long.valueOf(right));
    } else if (this == TEXT) {
      try {
        String left = new String(value.fieldValuebyte, "UTF-8");
        String right = new String(compareValue.fieldValuebyte, "UTF-8");
        return String.valueOf(left).compareTo(String.valueOf(right));
      } catch (Exception e) {
        return 0;
      }
    }
    throw new IllegalArgumentException();
  }
}