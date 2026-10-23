package lab.logstore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

final class AppendOnlyLogStoreIterator implements Iterator<LogStoreEntrySet> {

    private long storeLength = 0;
    private LogStoreEntryInput storeInput = null;

    public AppendOnlyLogStoreIterator(File logStore) {
        try {

            storeInput = new LogStoreEntryInput(logStore);
            storeLength = storeInput.length();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return posWithinBounds(storeInput.pos());
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public LogStoreEntrySet next() {
        return readNext();
    }

    private void close() {
        try {
            storeInput.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private LogStoreEntrySet readNext() {
        try {
            if (!posWithinBounds(storeInput.pos())) {
                return null;
            }

            Optional<LogStoreEntrySet> logResult = storeInput.getNextEntry();

            if (logResult.isPresent()) {
                Optional<LogStoreEntrySet> cdcResult = storeInput.getNextEntry();

                if (cdcResult.isPresent()) {
                    return logResult.get();
                }
            }

            return null;

        } catch (IOException e) {
            storeLength = 0;
            return null;
        } finally {
            closeIfEOF();
        }
    }

    private void closeIfEOF() {
        try {
            if (storeInput.pos() == storeLength) {
                close();
            }
        } catch (IOException e) {
            close();
        }
    }

    private boolean posWithinBounds(long currentPos) {
        return storeLength > 0 && currentPos >= 0 && currentPos < storeLength;
    }

}