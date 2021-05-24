package dbProject;
import java.util.HashMap;
import java.util.Map;

public enum PageType {
  INTERIOR((byte) 5),
  INTERIORINDEX((byte) 2),
  LEAF((byte) 13),
  LEAFINDEX((byte) 10);

  private static final Map < Byte,
  PageType > Lookup = new HashMap < Byte,
  PageType > ();

  static {
    for (PageType s: PageType.values())
    Lookup.put(s.getValue(), s);
  }

  private PageType(byte value) {
    this.value = value;
  }

  private byte value;

  public byte getValue() {
    return value;
  }

  public static PageType get(byte value) {
    return Lookup.get(value);
  }
}