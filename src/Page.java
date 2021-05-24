package dbProject;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Page {

  public PageType pageType;
  short noOfCells = 0; // probably want to handle this more dynamically
  public int pageNo;
  short contentStartOffset;
  public int rightPageNo;
  public int parentPageNo;
  private List<Cell> cells; // all page types will at least have the header just not always the body
  boolean refreshCells = false;
  long pageStart; // the page offset
  int availableSpace;
  RandomAccessFile file;

  public Page(RandomAccessFile file, int pageNo) {
    try {
      this.pageNo = pageNo;
      this.file = file;
      pageStart = Settings.getPageSize() * pageNo;
      file.seek(pageStart);
      pageType = PageType.get(file.readByte());
      file.readByte();
      noOfCells = file.readShort();
      contentStartOffset = file.readShort();
      availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

      rightPageNo = file.readInt();

      parentPageNo = file.readInt();

      file.readShort();

      fillCells();

    } catch (IOException ex) {
      System.out.println("! Error while reading the page " + ex.getMessage());
    }
  }

  // creates a new page at the end of the file
  public Page(RandomAccessFile file, PageType pageType, int rightPage, int parentPageNo) {
    try {
      Long page_val = Long.valueOf((file.length() / Settings.getPageSize()));
      pageNo = page_val.intValue();
      file.setLength(file.length() + Settings.getPageSize());
      file.seek(Settings.getPageSize() * pageNo);
      file.write(pageType.getValue());
      file.write(0x00);
      file.writeShort(0);
      file.writeShort((short) (Settings.getPageSize()));

      file.writeInt(rightPage);

      file.writeInt(parentPageNo);

      this.pageType = pageType;
      this.noOfCells = 0;
      this.cells = new ArrayList<Cell>();
      this.contentStartOffset = (short) Settings.getPageSize();
      this.rightPageNo = rightPage;
      this.parentPageNo = parentPageNo;
      this.pageStart = Settings.getPageSize() * pageNo; // the page offset
      this.availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);
      this.file = file;
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }

  public boolean isRoot() {
    return parentPageNo == -1;
  }

  public static PageType getPageType(RandomAccessFile file, int pageNo) throws IOException {
    try {
      int Begin_page = Settings.getPageSize() * pageNo;
      file.seek(Begin_page);
      return PageType.get(file.readByte());
    } catch (IOException ex) {
      System.out.println("ex.getMessage()");
      throw ex;
    }
  }

  public List<Cell> getPageCells() {

    if (refreshCells)
      fillCells();

    refreshCells = false;

    return cells;
  }

  public void clearCells() throws IOException {
    file.seek(pageStart);
    for (int i = 0; i < Settings.getPageSize(); i++) {
      file.writeByte(0);
    }
    file.seek(pageStart);
    file.write(pageType.getValue());
    file.write(0x00);
    file.writeShort(0);
    file.writeShort((short) (Settings.getPageSize()));
    file.writeInt(rightPageNo);
    file.writeInt(parentPageNo);
    this.noOfCells = 0;
    this.contentStartOffset = (short) Settings.getPageSize();
    this.availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);
    this.cells = new ArrayList<Cell>();
  }

  // essetially rewrite the page with the new cells;
  public void setPageCells(List<Cell> newCells) {
    try {
      clearCells();
      for (Cell cell : newCells) {
        try {
          addCell(cell);
        } catch (PageOverflowException e) {
          System.out.println("Page overflow when rewritting cells this should not happen!");
        }
      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }

  public Cell getLastCell() {
    List<Cell> cells = getPageCells();
    return cells.get(cells.size() - 1);
  }

  public List<Integer> getChildren() {
    List<Integer> pageNumbers = new ArrayList<Integer>();
    for (Cell cell : getPageCells()) {
      pageNumbers.add(cell.leftChildPageNo);
    }
    pageNumbers.add(rightPageNo);
    return pageNumbers;
  }

  public List<Page> getChildPages() {
    List<Page> pageNumbers = new ArrayList<Page>();
    for (Cell cell : getPageCells()) {
      if (cell.leftChildPageNo != -1) {
        pageNumbers.add(new Page(file, cell.leftChildPageNo));
      }
    }
    if (rightPageNo != -1) {
      pageNumbers.add(new Page(file, rightPageNo));
    }
    return pageNumbers;
  }

  // probably need to make this just the cell
  public Cell addCell(Cell newCell) throws PageOverflowException, IOException {
    Condition valueTest = new Condition(newCell, "=");
    for (Cell cell : cells) {
      if (valueTest.evaluate(cell.value))
        if (pageType == PageType.LEAF || pageType == PageType.INTERIOR) {
          return null;
        }
    }

    if (newCell.getPayloadSize() + 4 > availableSpace) {
      throw new PageOverflowException(this, newCell);
    }

    short cellStart = contentStartOffset;

    // might want to move this logic to the cell itself but this is fine rn
    short newCellStart = Integer.valueOf((cellStart - newCell.getPayloadSize() - 2)).shortValue();
    file.seek(pageNo * Settings.getPageSize() + newCellStart);

    newCell.setPosition(newCellStart, noOfCells, pageStart);

    byte[] cellHeader = Utils.Bytestobytes(newCell.cellHeader());
    file.write(cellHeader);

    byte[] cellBody = Utils.Bytestobytes(newCell.cellBody());
    if (cellBody != null) {
      file.write(cellBody);
    }

    file.seek(pageStart + 0x10 + (noOfCells * 2));
    file.writeShort(newCellStart);

    contentStartOffset = newCellStart;

    file.seek(pageStart + 4);
    file.writeShort(contentStartOffset);

    noOfCells++;
    file.seek(pageStart + 2);
    file.writeShort(noOfCells);

    availableSpace = contentStartOffset - 0x10 - (noOfCells * 2);

    refreshCells = true;
    cells.add(newCell);

    return newCell;
  }

  public void setParent(int parentPageNo) throws IOException {
    file.seek(Settings.getPageSize() * pageNo + 0x0A);
    file.writeInt(parentPageNo);
    this.parentPageNo = parentPageNo;
  }

  public void setRightPageNo(int rightPageNo) throws IOException {
    file.seek(Settings.getPageSize() * pageNo + 0x06);
    file.writeInt(rightPageNo);
    this.rightPageNo = rightPageNo;
  }

  private void fillCells() {
    cells = new ArrayList<Cell>(); // reset the cells
    for (short i = 0; i < noOfCells; i++) {
      switch (pageType) {
        case INTERIOR:
          cells.add(TableInteriorCell.read(file, pageType, pageStart, i));
          break;
        case LEAF:
          cells.add(TableLeafCell.read(file, pageType, pageStart, i));
          break;
        case INTERIORINDEX:
          cells.add(IndexCell.read(file, pageType, pageStart, i));
          break;
        case LEAFINDEX:
          cells.add(IndexCell.read(file, pageType, pageStart, i));
          break;
      }
    }
  }

  public void deleteCell(short cellIndex) {
    try {
      refreshCells = true;
      cells.remove(cellIndex);
      // shift the rest of the cells
      for (int i = cellIndex + 1; i < noOfCells; i++) {
        file.seek(pageStart + 0x10 + (i * 2));
        short cellStart = file.readShort();

        if (cellStart == 0)
          continue;

        file.seek(pageStart + 0x10 + ((i - 1) * 2));
        file.writeShort(cellStart);
      }

      noOfCells--;

      file.seek(pageStart + 2);
      file.writeShort(noOfCells);
      refreshCells = true;

    } catch (IOException e) {
      System.out.println("Error when deleting cell" + cellIndex + "in page " + pageNo);
    }
  }
}