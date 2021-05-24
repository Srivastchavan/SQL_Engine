package dbProject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TableLeafCell extends Cell {
    public short payloadSize;
    public byte noOfcolumns;

    public int rowId;
    public Byte[] colDatatypes;
    public Byte[] cellBody;
    private List<Attribute> attributes;

    public TableLeafCell(short payloadSize, int rowId, List<Attribute> attributes, short cellOffset, short cellIndex,
            long cellPosition) {
        this.attributes = attributes;
        this.noOfcolumns = (byte) attributes.size();
        this.payloadSize = payloadSize;
        this.rowId = rowId;
        this.cellOffset = cellOffset;
        this.cellIndex = cellIndex;
        this.cellPosition = cellPosition;
        this.value = new Attribute(DataType.INT, Integer.valueOf(rowId).toString());
    }

    public TableLeafCell(int rowId, List<Attribute> attributes) {
        this.attributes = attributes;
        this.noOfcolumns = (byte) attributes.size();
        this.payloadSize = getPayloadSize();
        this.rowId = rowId;
        this.value = new Attribute(DataType.INT, Integer.valueOf(rowId).toString());
    }

    public static TableLeafCell read(RandomAccessFile binaryFile, PageType pageType, long pageStart, short cellIndex) {
        try {
            binaryFile.seek(pageStart + 0x10 + (cellIndex * 2));
            short cellStart = binaryFile.readShort();
            if (cellStart == 0)
                return null;
            binaryFile.seek(pageStart + cellStart);

            short payloadSize = binaryFile.readShort();
            int rowId = binaryFile.readInt();
            byte noOfcolumns = binaryFile.readByte();

            byte[] colDatatypes = new byte[noOfcolumns];
            byte[] cellBody = new byte[payloadSize - noOfcolumns - 1];

            binaryFile.read(colDatatypes);
            binaryFile.read(cellBody);

            ArrayList<Attribute> attributes = new ArrayList<>();
            int pointer = 0;
            for (byte colDataType : colDatatypes) {
                byte[] fieldValue = Arrays.copyOfRange(cellBody, pointer, pointer + DataType.getLength(colDataType));
                attributes.add(new Attribute(DataType.get(colDataType), fieldValue));
                pointer = pointer + DataType.getLength(colDataType);
            }

            return new TableLeafCell(payloadSize, rowId, attributes, cellStart, cellIndex, pageStart + cellStart);
        } catch (IOException ex) {
            System.out.println("! Error while reading table leaf cell " + ex.getMessage());
        }
        return null;
    }

    public void update(RandomAccessFile binaryFile, Byte[] newValue, short ordinalPosition) throws IOException {
        binaryFile.seek(cellPosition + 7);
        int valueOffset = 0;
        // ordinal position is one index so start from 1
        for (int i = 1; i < ordinalPosition; i++) {
            valueOffset += DataType.getLength((byte) binaryFile.readByte());
        }

        binaryFile.seek(cellPosition + 7 + noOfcolumns + valueOffset);
        binaryFile.write(Utils.Bytestobytes(newValue));
    }

    public HashMap<Short, Attribute> getAttributes() {
        HashMap<Short, Attribute> attributeMap = new HashMap<>();
        short i = 1;
        for (Attribute attribute : attributes) {
            attributeMap.put(i, attribute);
            i++;
        }
        return attributeMap;
    }

    public Short getPayloadSize() {
        List<Byte> Cols_type = new ArrayList<Byte>();
        List<Byte> Body_rec = new ArrayList<Byte>();

        for (Attribute attribute : attributes) {
            Body_rec.addAll(Arrays.asList(attribute.fieldValueByte));

            if (attribute.dataType == DataType.TEXT) {
                Cols_type.add(Integer.valueOf(DataType.TEXT.getValue() + (attribute.toString().length())).byteValue());
            } else {
                Cols_type.add(attribute.dataType.getValue());
            }
        }

        Integer pl = Integer.valueOf(Body_rec.size() + Cols_type.size() + 7);
        return pl.shortValue();
    }

    public Byte[] cellHeader() {

        List<Byte> cellHeader = new ArrayList<Byte>();
        cellHeader.addAll(Arrays.asList(Utils.byteToBytes(
                ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(payloadSize).array())));

        cellHeader.addAll(Arrays.asList(Utils
                .byteToBytes(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(rowId).array())));

        return cellHeader.toArray(new Byte[cellHeader.size()]);

    }

    public Byte[] cellBody() {
        List<Byte> cellBody = new ArrayList<Byte>();

        // header
        cellBody.add(Integer.valueOf(noOfcolumns).byteValue());
        for (Attribute attribute : attributes) {
            if (attribute.dataType == DataType.TEXT) {
                cellBody.add(Integer.valueOf(DataType.TEXT.getValue() + (attribute.toString().length())).byteValue());
            } else {
                cellBody.add(attribute.dataType.getValue());
            }
        }
        // body
        for (Attribute attribute : attributes) {
            cellBody.addAll(Arrays.asList(attribute.fieldValueByte));
        }
        return cellBody.toArray(new Byte[cellBody.size()]);
    }
}
