package dbProject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class BPlusTree {

    RandomAccessFile file;
    int rootPageNo;
    String tableName;

    public BPlusTree(RandomAccessFile file, String tableName) {
        this.file = file;
        this.rootPageNo = Utils.getRootPageNo(file);
        this.tableName = tableName;
    }

    public Page getRootPage() {
        this.rootPageNo = Utils.getRootPageNo(file);
        return new Page(file, this.rootPageNo);
    }

    public List<Integer> getAllLeaves() throws IOException {

        List<Integer> leafPages = new ArrayList<>();
        file.seek(rootPageNo * Settings.pageSize);

        PageType rootPageType = PageType.get(file.readByte());
        if (rootPageType == PageType.LEAF) {
            if (!leafPages.contains(rootPageNo))
                leafPages.add(rootPageNo);
        } else {
            addLeaves(rootPageNo, leafPages);
        }

        return leafPages;

    }

    private void addLeaves(int interiorPageNo, List<Integer> leafPages) throws IOException {
        Page interiorPage = new Page(file, interiorPageNo);
        for (Integer pageNumber : interiorPage.getChildren()) {
            if (Page.getPageType(file, pageNumber) == PageType.LEAF) {
                if (!leafPages.contains(pageNumber))
                    leafPages.add(pageNumber);
            } else {
                addLeaves(pageNumber, leafPages);
            }
        }
    }

    public List<Integer> getAllLeaves(Condition condition) throws IOException {

        if (condition == null
                || !(new File("data/user_data/" + tableName + "_" + condition.columnName + ".ndx").exists())) {

            return getAllLeaves();
        }
        return null;
        /*
         * Create Index file
         */

    }

    // this does some special splitting that is a bit weird, it just keeps the full
    // page and adds the new cell to the right sibling
    // still stays balanced since row id is always increasing
    public void handlePageOverflow(Page page, Cell newCell) throws IOException {

        if (page.pageType == PageType.LEAF) {
            Page newRightLeafPage = new Page(file, PageType.LEAF, -1, -1);

            if (page.parentPageNo == -1) {
                // we have the root page so need to restructure things a bit
                Page newParentPage = new Page(file, PageType.INTERIOR, newRightLeafPage.pageNo, -1);

                newRightLeafPage.setParent(newParentPage.pageNo);
                newRightLeafPage.setRightPageNo(page.rightPageNo);

                page.setRightPageNo(newRightLeafPage.pageNo);
                page.setParent(newParentPage.pageNo);

                newParentPage.setRightPageNo(newRightLeafPage.pageNo);
                try {
                    newParentPage.addCell(new TableInteriorCell(newCell.value, page.pageNo));
                } catch (PageOverflowException pageException) {
                    // recursively handle the exception
                    handlePageOverflow(pageException.page, pageException.newCell);
                }
            } else {
                Page parentPage = new Page(file, page.parentPageNo);

                newRightLeafPage.setParent(parentPage.pageNo);
                newRightLeafPage.setRightPageNo(page.rightPageNo);

                page.setRightPageNo(newRightLeafPage.pageNo);

                parentPage.setRightPageNo(newRightLeafPage.pageNo);
                try {
                    parentPage.addCell(new TableInteriorCell(newCell.value, page.pageNo));
                } catch (PageOverflowException pageException) {
                    // recursively handle the exception
                    handlePageOverflow(pageException.page, pageException.newCell);
                }
            }
            try {
                newRightLeafPage.addCell(newCell);
            } catch (PageOverflowException pageException) {
                // recursively handle the exception
                handlePageOverflow(pageException.page, pageException.newCell);
            }
        } else if (page.pageType == PageType.INTERIOR) {
            Page parentPage = new Page(file, page.parentPageNo);
            Page newRightPage = new Page(file, PageType.INTERIOR, -1, -1);

            newRightPage.setParent(parentPage.pageNo);
            newRightPage.setRightPageNo(page.rightPageNo);

            page.setRightPageNo(newRightPage.pageNo);
            page.setParent(parentPage.pageNo);

            parentPage.setRightPageNo(newRightPage.pageNo);

            try {
                newRightPage.addCell(newCell);
                parentPage.addCell(new TableInteriorCell(newCell.value, page.pageNo));
            } catch (PageOverflowException pageException) {
                // recursively handle the exception
                handlePageOverflow(pageException.page, pageException.newCell);
            }
        }
    }

    private int binarySearch(List<Cell> values, int searchValue, int start, int end) {
        if (end - start <= 2) {
            int i = start;
            for (i = start; i < end; i++) {
                if (Integer.parseInt(values.get(i).value.toString()) < searchValue)
                    continue;
                else
                    break;
            }
            return i;
        } else {

            int mid = (end - start) / 2 + start;
            if (Integer.parseInt(values.get(mid).value.toString()) == searchValue)
                return mid;

            if (Integer.parseInt(values.get(mid).value.toString()) < searchValue)
                return binarySearch(values, searchValue, mid + 1, end);
            else
                return binarySearch(values, searchValue, start, mid - 1);

        }
    }

    public int getPageNo(int rowId, Page page) {
        if (page.pageType == PageType.LEAF)
            return page.pageNo;

        int index = binarySearch(page.getPageCells(), rowId, 0, page.noOfCells - 1);

        if (rowId < Integer.parseInt(page.getPageCells().get(index).value.toString())) {
            return getPageNo(rowId, new Page(file, page.getPageCells().get(index).leftChildPageNo));
        } else {
            if (index + 1 < page.getPageCells().size())
                return getPageNo(rowId, new Page(file, page.getPageCells().get(index + 1).leftChildPageNo));
            else
                return getPageNo(rowId, new Page(file, page.rightPageNo));

        }
    }

    public int getPageNo(int rowId) {
        return getPageNo(rowId, new Page(file, rootPageNo));
    }

    public Set<Integer> getPageNumbers(List<Integer> rowIds) {
        Set<Integer> pageNumbers = new TreeSet<Integer>();
        for (int rowId : rowIds) {
            pageNumbers.add(getPageNo(rowId));
        }
        return pageNumbers;
    }

    public Page getPageForInsert(int rootPageNo) {
        Page rootPage = new Page(file, rootPageNo);
        if (rootPage.pageType != PageType.LEAF)
            // since row id is always increasing go all the way to the right
            return getPageForInsert(rootPage.rightPageNo);
        else
            return rootPage;
    }

    public Page getPageForInsert() {
        return getPageForInsert(rootPageNo);
    }

}
