package dbProject;
import java.util.ArrayList;
import java.util.List;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class BTree {

    RandomAccessFile file;
    int rootPageNo;
    String tableName;
    String columnName;
    String filename;
    boolean exists = true;

    public BTree(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.filename = "data/user_data/" + tableName + "_" + columnName + ".ndx";
        if (!new File(filename).exists()) {
            this.exists = false;
            return;
        }
        try {
            this.file = new RandomAccessFile(filename, "rw");
            this.rootPageNo = Utils.getRootPageNo(file);
        } catch (IOException e) {
            this.exists = false;
        }
    }

    public Page getRootPage() {
        this.rootPageNo = Utils.getRootPageNo(file);
        return new Page(file, this.rootPageNo);
    }

    // this does some special splitting that is a bit weird, it just keeps the full
    // page and adds the new cell to the right sibling
    // still stays balanced since row id is always increasing
    public void handlePageOverflow(Page page, Cell newCell) throws IOException {
        Page parentPage;
        if (page.isRoot()) {
            parentPage = new Page(file, PageType.INTERIORINDEX, -1, -1);
        } else {
            parentPage = new Page(file, page.parentPageNo);
        }
        // TODO: make sure that the pageType makes sense for the new right page
        Page newRightPage = new Page(file, page.pageType, -1, -1);

        newRightPage.setParent(parentPage.pageNo);
        newRightPage.setRightPageNo(page.rightPageNo);

        page.setRightPageNo(newRightPage.pageNo);
        page.setParent(parentPage.pageNo);

        parentPage.setRightPageNo(newRightPage.pageNo);

        // add the new cell to the list and split it in the middle
        List<Cell> cells = page.getPageCells();
        cells.add(newCell);
        Integer midIndex = (cells.size()) / 2;
        List<Cell> left = new ArrayList<Cell>(cells.subList(0, midIndex));
        Cell midCell = cells.get(midIndex);
        List<Cell> right = new ArrayList<Cell>(cells.subList(midIndex + 1, cells.size()));

        page.setPageCells(left);
        newRightPage.setPageCells(right);
        midCell.leftChildPageNo = page.pageNo;

        try {
            parentPage.addCell(midCell);
        } catch (PageOverflowException pageException) {
            // recursively handle the exception
            handlePageOverflow(pageException.page, pageException.newCell);
        }
    }

    public List<Integer> getAllRowIds(int pageNo) {
        if (pageNo == -1) {
            return new ArrayList<Integer>();
        }
        List<Integer> rowIds = new ArrayList<Integer>();
        Page page = new Page(file, pageNo);
        for (Cell cell : page.getPageCells()) {
            IndexCell indexCell = (IndexCell) cell;
            rowIds.addAll(indexCell.rowIds);
            rowIds.addAll(getAllRowIds(indexCell.leftChildPageNo));
        }
        rowIds.addAll(getAllRowIds(page.rightPageNo));
        return rowIds;
    }

    public List<Integer> findRowIds(int rootPageNo, Condition condition) {
        if (rootPageNo == -1) {
            return new ArrayList<Integer>();
        }
        Condition lt = new Condition(condition.value, "<");
        Condition eq = new Condition(condition.value, "=");
        Condition gt = new Condition(condition.value, ">");
        Page rootPage = new Page(file, rootPageNo);
        if (condition.condition.equals("=")) {
            // if = then we need to find the value if it exists;
            for (Cell cell : rootPage.getPageCells()) {
                IndexCell indexCell = (IndexCell) cell;
                if (gt.evaluate(cell.value)) {
                    return findRowIds(cell.leftChildPageNo, condition);
                } else if (eq.evaluate(cell.value)) {
                    return indexCell.rowIds;
                }
            }
            if (rootPage.pageType != PageType.LEAFINDEX) {
                return findRowIds(rootPage.rightPageNo, condition);
            }

        } else if (condition.condition.equals("<")) {
            List<Integer> rowIds = new ArrayList<Integer>();
            boolean matched = false;
            for (Cell cell : rootPage.getPageCells()) {
                IndexCell indexCell = (IndexCell) cell;
                if (gt.evaluate(cell.value)) {
                    rowIds.addAll(indexCell.rowIds);
                    rowIds.addAll(getAllRowIds(cell.leftChildPageNo));
                } else if (eq.evaluate(cell.value) || lt.evaluate(cell.value)) {
                    matched = true;
                    rowIds.addAll(findRowIds(cell.leftChildPageNo, condition));
                }
            }
            if (!matched) {
                rowIds.addAll(findRowIds(rootPage.rightPageNo, condition));
            }
            return rowIds;
        } else if (condition.condition.equals(">")) {
            List<Integer> rowIds = new ArrayList<Integer>();
            boolean matched = false;
            for (Cell cell : rootPage.getPageCells()) {
                IndexCell indexCell = (IndexCell) cell;
                if (lt.evaluate(cell.value)) {
                    matched = true;
                    rowIds.addAll(indexCell.rowIds);
                    rowIds.addAll(findRowIds(cell.leftChildPageNo, condition));
                }
            }
            if (matched) {
                rowIds.addAll(getAllRowIds(rootPage.rightPageNo));
            }
            return rowIds;
        } else if (condition.condition.equals("<=")) {
            List<Integer> rowIds = new ArrayList<Integer>();
            rowIds.addAll(findRowIds(rootPageNo, eq));
            rowIds.addAll(findRowIds(rootPageNo, lt));
            return rowIds;
        } else if (condition.condition.equals(">=")) {
            List<Integer> rowIds = new ArrayList<Integer>();
            rowIds.addAll(findRowIds(rootPageNo, eq));
            rowIds.addAll(findRowIds(rootPageNo, gt));
            return rowIds;
        } else if (condition.condition.equals("!=") || condition.condition.equals("<>")) {
            List<Integer> rowIds = new ArrayList<Integer>();
            rowIds.addAll(findRowIds(rootPageNo, gt));
            rowIds.addAll(findRowIds(rootPageNo, lt));
            return rowIds;
        }

        return new ArrayList<Integer>();
    }

    public List<Integer> findRowIds(Condition condition) {
        return findRowIds(rootPageNo, condition);
    }

    public Page getPageForInsert(int rootPageNo, Attribute newValue) {
        Condition gt = new Condition(newValue, ">");
        Condition eq = new Condition(newValue, "=");
        Page rootPage = new Page(file, rootPageNo);
        if (rootPage.pageType == PageType.LEAFINDEX) {
            return rootPage;
        } else {
            for (Cell cell : rootPage.getPageCells()) {
                if (gt.evaluate(cell.value)) {
                    return getPageForInsert(cell.leftChildPageNo, newValue);
                } else if (eq.evaluate(cell.value)) {
                    return rootPage;
                }
            }
            return getPageForInsert(rootPage.rightPageNo, newValue);
        }
    }

    public Page getPageForInsert(Attribute newValue) {
        return getPageForInsert(Utils.getRootPageNo(file), newValue);
    }

    public void insertValue(Attribute value, int rowId) throws IOException {
        Page indexPage = getPageForInsert(value);
        Condition eq = new Condition(value, "=");
        boolean foundCell = false;
        List<Cell> newCells = new ArrayList<>();
        for (Cell cell : indexPage.getPageCells()) {
            IndexCell indexCell = (IndexCell) cell;
            if (eq.evaluate(indexCell.value)) {
                foundCell = true;
                indexCell.addRowId(rowId);
            }
            newCells.add(indexCell);
        }
        if (foundCell) {
            indexPage.clearCells();
            for (Cell cell : newCells) {
                try {
                    indexPage.addCell(cell);
                } catch (PageOverflowException pageException) {
                    handlePageOverflow(pageException.page, pageException.newCell);
                }
            }
        } else {
            try {
                indexPage.addCell(new IndexCell(value, rowId));
            } catch (PageOverflowException pageException) {
                handlePageOverflow(pageException.page, pageException.newCell);
            }
        }
    }

    public void deleteRowId(int rowId, Attribute value) throws IOException {
        Page indexPage = getPageForInsert(value);
        Condition eq = new Condition(value, "=");

        List<Cell> newCells = new ArrayList<>();
        boolean foundCell = false;
        for (Cell cell : indexPage.getPageCells()) {
            IndexCell indexCell = (IndexCell) cell;
            if (eq.evaluate(indexCell.value)) {
                indexCell.removeRowId(rowId);
                foundCell = true;
                // might need to handle cell/page removal but I kinda think not
            }
            newCells.add(indexCell);
        }
        if (foundCell) {
            indexPage.clearCells();
            for (Cell cell : newCells) {
                try {
                    indexPage.addCell(cell);
                } catch (PageOverflowException pageException) {
                    handlePageOverflow(pageException.page, pageException.newCell);
                }
            }
        }
    }

    public void updateRowId(int rowId, Attribute oldValue, Attribute newValue) throws IOException {
        deleteRowId(rowId, oldValue);
        insertValue(newValue, rowId);
    }

}
