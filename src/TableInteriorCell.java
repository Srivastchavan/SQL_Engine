package dbProject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableInteriorCell extends Cell {
    public int rowId;

    public TableInteriorCell(Attribute attribute, int leftChildPageNo) {
        this.value = attribute;
        this.rowId = Integer.parseInt(attribute.toString());
        this.leftChildPageNo = leftChildPageNo;
    }

    public TableInteriorCell(int rowId, int leftChildPageNo) {
        this.value = new Attribute(DataType.INT, Integer.toString(rowId));
        this.rowId = rowId;
        this.leftChildPageNo = leftChildPageNo;
    }

    public TableInteriorCell(int rowId, int leftChildPageNo, short cellOffset, short cellIndex, long pageStart) {
        this.value = new Attribute(DataType.INT, Integer.toString(rowId));
        this.rowId = rowId;
        this.leftChildPageNo = leftChildPageNo;
        setPosition(cellOffset, cellIndex, pageStart);
    }

    public static TableInteriorCell read(RandomAccessFile binaryFile, PageType pageType, long pageStart,
            short cellIndex) {
        try {
            binaryFile.seek(pageStart + 0x10 + (cellIndex * 2));
            short cellStart = binaryFile.readShort();
            if (cellStart == 0)
                return null;
            binaryFile.seek(pageStart + cellStart);

            int leftChildPageNo = binaryFile.readInt();
            int rowId = binaryFile.readInt();
            return new TableInteriorCell(rowId, leftChildPageNo, cellStart, cellIndex, pageStart);
        } catch (IOException ex) {
            System.out.println("! Error while reading table interior cell " + ex.getMessage());
        }
        return null;
    }

    public Short getPayloadSize() {
        return 8;
    }

    public Byte[] cellHeader() {
        List<Byte> cellHeader = new ArrayList<>();

        cellHeader.addAll(Arrays.asList(Utils.byteToBytes(
                ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(leftChildPageNo).array())));
        cellHeader.addAll(Arrays.asList(Utils.byteToBytes(
                ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).put(value.fieldValuebyte).array())));

        return cellHeader.toArray(new Byte[cellHeader.size()]);

    }

}
