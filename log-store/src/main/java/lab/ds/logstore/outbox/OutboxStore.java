package lab.ds.logstore.outbox;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lab.ds.logstore.CdcEntry;
import lab.ds.logstore.CdcRecord;
import lab.ds.logstore.LogStoreEntrySet;
import lab.ds.logstore.AppendOnlyLogStore.StoreReader;
import lab.ds.logstore.core.LogStoreEvent;

public final class OutboxStore implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(OutboxStore.class);

    private final StoreReader storeReader;
    private final Path outboxPath;
    private final Path tmpOutboxPath;

    private final List<Function<LogStoreEvent, Void>> outboxListeners;

    private volatile List<UnpublishedOutboxEntry> unpublishedEntries;

    private final Object stateMonitor;
    private final ScheduledExecutorService backupExecutorService;

    public OutboxStore(StoreReader reader, File logStore) throws IOException {
        storeReader = reader;

        if (logStore == null || !logStore.isFile()) {
            throw new IllegalArgumentException("Invalid store file. outboxStore must be a file.");
        }

        String logStoreName = logStore.getName();

        logStoreName = logStoreName.replace("wal-", "");
        logStoreName = logStoreName.replace(".db", "");

        String outboxFileName = "outbox-" + logStoreName;

        outboxPath = Paths.get(logStore.getParent(), outboxFileName + ".bin");
        tmpOutboxPath = Paths.get(logStore.getParent(), outboxFileName + ".tmp");

        unpublishedEntries = new LinkedList<>();
        outboxListeners = new LinkedList<>();

        stateMonitor = new Object();

        backupExecutorService = Executors.newScheduledThreadPool(1);
        backupExecutorService.scheduleWithFixedDelay(this.new OutboxBackupThread(), 0, 3, TimeUnit.SECONDS);

        loadState();
    }

    @Override
    public void close() throws IOException {
        backupExecutorService.shutdownNow();
        saveUnpublishedOutbox();
    }

    public void recordInserted(LogStoreEntrySet cdcRecordSet) {
        synchronized (stateMonitor) {
            unpublishedEntries
                    .add(new UnpublishedOutboxEntry(((CdcEntry) cdcRecordSet.entry()).record().timestamp(),
                            cdcRecordSet.offset()));
        }
    }

    public void registerListener(Function<LogStoreEvent, Void> listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener cannot be null.");
        }

        outboxListeners.add(listener);
    }

    public void publishEvents() {
        synchronized (stateMonitor) {
            notifyListeners();
        }
    }

    private void loadState() throws IOException {
        if (!Files.exists(outboxPath) || Files.size(outboxPath) == 0) {
            return;
        }

        try (RandomAccessFile outbox = new RandomAccessFile(outboxPath.toFile(), "r")) {
            int size = outbox.readInt();

            if (size <= 0) {
                return;
            }

            List<UnpublishedOutboxEntry> entries = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                entries.add(UnpublishedOutboxEntry.createFromInput(outbox));
            }

            synchronized (stateMonitor) {
                unpublishedEntries = entries;
            }
        }
    }

    private void notifyListeners() {
        unpublishedEntries
                .stream()
                .forEach(entry -> {
                    long eventId = entry.id();
                    long offset = entry.offset();

                    Optional<CdcRecord> cdcRecord = Optional.empty();

                    try {
                        cdcRecord = storeReader.readCdcRecordAt(offset);
                    } catch (IOException e) {
                        logger.error("Error while fetching CDC entry for log at offset " +
                                offset + ": "
                                + e.getMessage(), e);
                    }

                    if (cdcRecord.isPresent() && cdcRecord.get().timestamp() == eventId) {
                        final Optional<CdcRecord> theRecord = cdcRecord;

                        outboxListeners.forEach(listener -> {
                            try {
                                listener.apply((LogStoreEvent) theRecord.get().event());
                            } catch (Exception e) {
                                logger.error("Ignoring the error that occurred whilst calling outbox listener: "
                                        + e.getMessage(), e);
                            }
                        });
                    }
                });
        unpublishedEntries.clear();
    }

    private void saveUnpublishedOutbox() {
        synchronized (stateMonitor) {
            try (OutputStream stream = Files.newOutputStream(tmpOutboxPath, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

                if (!unpublishedEntries.isEmpty()) {
                    int totalBytes = Integer.BYTES
                            + (unpublishedEntries.size() * (UnpublishedOutboxEntry.BYTES));

                    ByteBuffer outboxBuffer = ByteBuffer.allocate(totalBytes);

                    outboxBuffer.putInt(unpublishedEntries.size());

                    unpublishedEntries.forEach(entry -> entry.writeEntry(outboxBuffer));

                    stream.write(outboxBuffer.array());
                    stream.flush();
                }

            } catch (IOException e) {
                logger.error("An error occurred whilst save temporary outbox: " + e.getMessage(), e);
            }

            try {
                Files.move(tmpOutboxPath, outboxPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                logger.error("An error occurred whilst saving outbox: " + e.getMessage(), e);
            }
        }
    }

    private class OutboxBackupThread extends Thread {

        private OutboxBackupThread() {
            super();
            setDaemon(true);
        }

        @Override
        public void run() {
            saveUnpublishedOutbox();
        }

    }

}