package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.HeapPage;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId id = (HeapPageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            byte[] pageBuf = new byte[BufferPool.getPageSize()];
            if (bis.skip((long) id.getPageNumber() * BufferPool.getPageSize()) != (long) id
                    .getPageNumber() * BufferPool.getPageSize()) {
                throw new IllegalArgumentException(
                        "Unable to seek to correct place in heapfile");
            }
            int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from heapfile");
            }
            Debug.log(1, "HeapFile.readPage: read page %d", id.getPageNumber());
            return new HeapPage(id, pageBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    // see DbFile.java for javadocs

    private Map<PageId, Long> pageOffsetsMap = new HashMap<>();

    public void writePage(Page page) throws IOException {
        // Cast the page to a HeapPage
        HeapPage heapPage = (HeapPage) page;

        // Get the page number from the page ID
        int pageNumber = heapPage.getId().getPageNumber();

        // Compute the file offset of the page
        long pageOffset = pageOffsetsMap.getOrDefault(heapPage.getId(), (long) pageNumber * BufferPool.getPageSize());

        // Create a RandomAccessFile to write the page to disk
        RandomAccessFile raf = new RandomAccessFile(f, "rw");

        // Seek to the file offset where the page will be written
        raf.seek(pageOffset);

        // Write the page data to the file
        raf.write(heapPage.getPageData());

        // Close the RandomAccessFile
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // Create an empty list to hold the pages that were modified
        List<Page> modifiedPages = new ArrayList<>();

        // Get the number of pages in the file
        int pageCount = numPages();

        // Keep track of the index of the page being checked for available space
        int pageIndex = 0;

        // Iterate over each page in the file to find one with free space for the tuple
        while (pageIndex < pageCount) {
            // Create a HeapPageId for the current page
            HeapPageId pageId = new HeapPageId(tableid, pageIndex);

            // Get the current page from the buffer pool with read-write permissions
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);

            // Check the number of unused slots on the current page
            int unusedSlots = page.getNumUnusedSlots();
            int totalSlots = page.getNumTuples();
            if (unusedSlots > 0) {
                // If the current page has free space, insert the tuple and add the modified page to the list
                page.insertTuple(t);
                modifiedPages.add(page);
                return modifiedPages;
            }

            pageIndex++;
        }

        // If we got here, we were unable to insert the tuple into any existing page.
        // Create a new page and insert the tuple into it.
        HeapPageId newPageId = new HeapPageId(tableid, pageCount);
        HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);

        // Write the new page to disk and add it to the list of modified pages
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

        try {
            bufferedOutputStream.write(newPage.getPageData());
            writePage(newPage);
            modifiedPages.add(newPage);
        } finally {
            bufferedOutputStream.close();
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // Create an empty list to hold the pages that were modified
        List<Page> modifiedPages = new ArrayList<>();

        // Get the page that contains the tuple and delete the tuple from it
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // Add the modified page to the list and return it
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    final TransactionId tid;
    final HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
