package dbProject;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class IndexCell extends Cell {
    public short payloadSize;
    public List<Integer> rowIds;
    public Byte noOfRowIds;
    public DataType dataType;

    // constructor for the leaf cell
    public IndexCell(Attribute attribute, Integer rowId) {
        this.noOfRowIds = (byte) 1;
        this.dataType = attribute.dataType;
        this.rowIds = new ArrayList<>();
        rowIds.add(rowId);
        this.value = attribute;
    }

    // constructor for the interior cell
    public IndexCell(Attribute attribute, List<Integer> rowIds, int leftPageNo) {
        this.noOfRowIds = (byte) rowIds.size();
        this.dataType = attribute.dataType;
        this.rowIds = rowIds;

        this.leftChildPageNo = leftPageNo;

        this.value = attribute; // use the value here
        this.payloadSize = getPayloadSize();
    }

    public IndexCell(DataType dataType, byte[] indexValue, List<Integer> rowIds, int leftPageNo) {
        this.noOfRowIds = (byte) rowIds.size();
        this.dataType = dataType;
        this.rowIds = rowIds;

        this.leftChildPageNo = leftPageNo;

        this.value = new Attribute(this.dataType, indexValue); // use the value here
        this.payloadSize = getPayloadSize();
    }

    public IndexCell(DataType dataType, byte[] indexValue, List<Integer> rowIds, int leftPageNo, short cellOffset,
            short cellIndex, long pageStart, short payloadSize) {
        this.noOfRowIds = (byte) rowIds.size();
        this.dataType = dataType;
        this.rowIds = rowIds;
        this.payloadSize = payloadSize;
        this.leftChildPageNo = leftPageNo;

        this.value = new Attribute(this.dataType, indexValue); // use the value here
        setPosition(cellOffset, cellIndex, pageStart);
    }

    public static IndexCell read(RandomAccessFile binaryFile, PageType pageType, long pageStart, short cellIndex) {
        try {
            binaryFile.seek(pageStart + 0x10 + (cellIndex * 2));
            short cellStart = binaryFile.readShort();
            if (cellStart == 0)
                return null;
            binaryFile.seek(pageStart + cellStart);

            int leftPageNo = -1;
            if (pageType == PageType.INTERIORINDEX)
                leftPageNo = binaryFile.readInt();

            short payloadSize = binaryFile.readShort();
            byte noOfRowIds = binaryFile.readByte();
            byte dataType = binaryFile.readByte();

            byte[] indexValue = new byte[DataType.getLength(dataType)];
            binaryFile.read(indexValue);

            List<Integer> lstRowIds = new ArrayList<>();
            for (int j = 0; j < noOfRowIds; j++) {
                lstRowIds.add(binaryFile.readInt());
            }

            return new IndexCell(DataType.get(dataType), indexValue, lstRowIds, leftPageNo, cellStart, cellIndex,
                    pageStart, payloadSize);
        } catch (IOException ex) {
            System.out.println("! Error while reading index cell " + ex.getMessage());
        }
        return null;
    }

    public Short getPayloadSize() {
        Integer pl = (leftChildPageNo == -1 ? 0 : 4) + 4 + Arrays.asList(value.fieldValueByte).size()
                + rowIds.size() * 4;
        return pl.shortValue();
    }

    public Byte[] cellHeader() {
        List<Byte> cellHeader = new ArrayList<>();

        if (leftChildPageNo != -1) {
            cellHeader.addAll(Arrays.asList(Utils.byteToBytes(
                    ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(leftChildPageNo).array())));
        }

        cellHeader.addAll(Arrays.asList(Utils.byteToBytes(
                ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(payloadSize).array())));

        return cellHeader.toArray(new Byte[cellHeader.size()]);
    }

    public Byte[] cellBody() {
        List<Byte> cellBody = new ArrayList<>();
        cellBody.add(Integer.valueOf(rowIds.size()).byteValue());
        if (value.dataType == DataType.TEXT) {
            cellBody.add(Integer.valueOf(DataType.TEXT.getValue() + (value.toString().length())).byteValue());
        } else {
            cellBody.add(value.dataType.getValue());
        }
        cellBody.addAll(Arrays.asList(value.fieldValueByte));
        for (Integer rowId : rowIds) {
            cellBody.addAll(Arrays.asList(Utils.byteToBytes(
                    ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(rowId).array())));
        }

        return cellBody.toArray(new Byte[cellBody.size()]);
    }

    public void addRowId(int rowId) {
        rowIds.add(rowId);
        noOfRowIds++;
    }

    public void removeRowId(int rowId) {
        if (rowIds.indexOf(rowId) != -1) {
            rowIds.remove(rowIds.indexOf(rowId));
            noOfRowIds--;
        }
    }

}