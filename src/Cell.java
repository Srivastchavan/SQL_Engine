package dbProject;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Cell {
    // the child of this page, stored on cell level
    // -1 if it is a leaf node
    public int leftChildPageNo = -1;
    protected long cellPosition = 0; // this is the position of this particular cell 0 if unknown
    protected short cellOffset = 0;
    protected short cellIndex = 0;
    public Attribute value; // this is the data of the cell used for comparisons

    public static Cell read(RandomAccessFile binaryFile, PageType pageType, long pageStart, short cellIndex) {
        return null;
    };

    public void update(RandomAccessFile binaryFile, Byte[] newValue) throws IOException {
        return;
    }

    public Byte[] cellHeader() {
        return null;
    }

    public Byte[] cellBody() {
        return null;
    }

    // used to assist move or setting after initial creation
    public void setPosition(short cellOffset, short cellIndex, long pageStart) {
        this.cellOffset = cellOffset;
        this.cellIndex = cellIndex;
        this.cellPosition = pageStart + cellOffset;
    }

    public Short getPayloadSize() {
        return 0;
    }

}
