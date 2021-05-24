package dbProject;


import java.util.Date;
import java.text.SimpleDateFormat;
//import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Attribute {
  public byte[] fieldValuebyte;
  public Byte[] fieldValueByte;
  public DataType dataType;
  // public String fieldValue;

  public static Byte[] byteToBytes(final byte[] data) {
    int length = 0;
    if (data == null)
      length = 0;
    else
      length = data.length;
    Byte[] result = new Byte[length];
    for (int i = 0; i < length; i++)
      result[i] = data[i];
    return result;
  }

  public static byte[] Bytestobytes(final Byte[] data) {
    int length = 0;
    if (data == null)
      length = 0;
    else
      length = data.length;
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++)
      result[i] = data[i];
    return result;
  }

  Attribute(DataType dataType, byte[] fieldValue) {
    this.dataType = dataType;
    this.fieldValuebyte = fieldValue;
    this.fieldValueByte = byteToBytes(fieldValuebyte);
  }

  Attribute(DataType dataType, String fieldValue) {
    this.dataType = dataType;
    // return fieldValue;

    try {
      switch (dataType) {
        case NULL:
          this.fieldValuebyte = null;
          break;

        case TINYINT:
          this.fieldValuebyte = new byte[] { Byte.parseByte(fieldValue) };
          break;

        case SMALLINT:
          Short data = Short.parseShort(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(data).array();
          break;

        case INT:
          Integer data_int = Integer.parseInt(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(data_int).array();
          break;

        case BIGINT:
          Long data_long = Long.parseLong(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Long.BYTES).putLong(data_long).array();
          break;

        case FLOAT:
          Float data_float = Float.parseFloat(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Float.BYTES).putFloat(data_float).array();
          break;

        case DOUBLE:
          Double data_double = Double.parseDouble(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Double.BYTES).putDouble(data_double).array();
          break;

        case YEAR:
          this.fieldValuebyte = new byte[] { (byte) (Integer.parseInt(fieldValue) - 2000) };
          break;

        case TIME:
          Integer Int_data = Integer.parseInt(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(Int_data).array();
          break;

        case DATETIME:
          SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
          Date datetime = sdftime.parse(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Long.BYTES).putLong(datetime.getTime()).array();
          break;

        case DATE:
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          Date date = sdf.parse(fieldValue);
          this.fieldValuebyte = ByteBuffer.allocate(Long.BYTES).putLong(date.getTime()).array();
          break;

        case TEXT:
          this.fieldValuebyte = fieldValue.getBytes();
          break;

        default:
          this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII);
          break;
      }
      this.fieldValueByte = byteToBytes(fieldValuebyte);
    } catch (Exception e) {
      System.out.println("! Cannot convert " + fieldValue + " to " + dataType.toString());
    }
  }

  @Override
  public String toString() {
    try {
      switch (dataType) {
        case NULL:
          return "NULL";
        case TINYINT:
          return Byte.valueOf(ByteBuffer.wrap(fieldValuebyte).get()).toString();
        case SMALLINT:
          return Short.valueOf(ByteBuffer.wrap(fieldValuebyte).getShort()).toString();
        case INT:
          return Integer.valueOf(ByteBuffer.wrap(fieldValuebyte).getInt()).toString();
        case BIGINT:
          return Long.valueOf(ByteBuffer.wrap(fieldValuebyte).getLong()).toString();
        case FLOAT:
          return Float.valueOf(ByteBuffer.wrap(fieldValuebyte).getFloat()).toString();
        case DOUBLE:
          return Double.valueOf(ByteBuffer.wrap(fieldValuebyte).getDouble()).toString();
        case YEAR:
          return Integer.valueOf((int) Byte.valueOf(ByteBuffer.wrap(fieldValuebyte).get()) + 2000).toString();
        case TIME:
          int millisSinceMidnight = ByteBuffer.wrap(fieldValuebyte).getInt() % 86400000;
          int ss = millisSinceMidnight / 1000;
          int hr = ss / 3600;
          int remhrss = ss % 3600;
          int mm = remhrss / 60;
          int remss = remhrss % 60;
          return String.format("%02d", hr) + ":" + String.format("%02d", mm) + ":" + String.format("%02d", remss);
        case DATETIME:
          SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
          Date dt = new Date(Long.valueOf(ByteBuffer.wrap(fieldValuebyte).getLong()));
          return dateTimeFormat.format(dt);
        case DATE:
          SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
          Date dt1 = new Date(Long.valueOf(ByteBuffer.wrap(fieldValuebyte).getLong()));
          return dateFormat.format(dt1);
        case TEXT:
          return new String(fieldValuebyte, "UTF-8");
        default:
          return new String(fieldValuebyte, "UTF-8");
      }
    } catch (Exception ex) {
      System.out.println("! Error in Format please check!!");
      System.out.println(ex);
    }
    return "ERROR";
  }

}