package dbProject;
public class PageOverflowException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = -8036817321458881868L;
    Page page;
    Cell newCell;

    // just a stub to be able to detect page overflows and handle them
    PageOverflowException(Page page, Cell newCell) {
        this.page = page;
        this.newCell = newCell;
    }
}
