package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class FTPSourceTask extends SourceTask {
    private FTPService ftpService;
    private String topic;
    private Queue<String> fileProcessingQueue = new ConcurrentLinkedQueue<>();
    private Map<String, FileProcessStatus> fileStatuses = new ConcurrentHashMap<>();
    private ExecutorService executorService;
    private volatile boolean running = true;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.topic = props.get("topic");
        OffsetStorageReader offsetStorageReader = this.context.offsetStorageReader();
        ftpService = new FTPService(props, offsetStorageReader);

        try {
            ftpService.connect();
            ftpService.loadTimestampFromOffsets();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 파일 큐 초기화
        initializeFileQueue();

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            while (running) {
                try {
                    updateFileQueueAndStatuses();
                    Thread.sleep(10000); // 10초 간격
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    log.error("Error updating file queue: ", e);
                }
            }
        });
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        if (fileProcessingQueue.isEmpty()) {
            return records;
        }

        while (!fileProcessingQueue.isEmpty()) {
            String fileName = fileProcessingQueue.poll();
            FileProcessStatus status = fileStatuses.get(fileName);

            try {
                List<String> lines = ftpService.readFileByLine(fileName);
                if (lines.isEmpty()) {
                    status.setProcessed(true);
                    continue;
                }
                log.info("Processing file: {}", fileName);

                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap("filename", fileName));
                long lastReadLine = offset != null ? (Long) offset.get("lineNumber") : -1;

                for (int lineNumber = 0; lineNumber < lines.size(); lineNumber++) {
                    if (lineNumber > lastReadLine) {
                        continue;
                    }

                    String line = lines.get(lineNumber);

                    Map<String, String> sourcePartition = Collections.singletonMap("filename", fileName);
                    Map<String, Long> sourceOffset = Collections.singletonMap("lineNumber", (long) lineNumber);

                    SourceRecord record = new SourceRecord(
                            sourcePartition,
                            sourceOffset,
                            this.topic,
                            Schema.STRING_SCHEMA,
                            line
                    );

                    records.add(record);
                }

                // 파일 처리 완료 후 상태 업데이트
                status.setLastProcessedTime(System.currentTimeMillis());
                status.setProcessed(true);
            } catch (IOException e) {
                log.error("Error processing file {}: ", fileName, e);
                // 처리 중 오류 발생 시, 상태를 업데이트하지 않음
            }
        }

        return records;
    }


    private void initializeFileQueue() {
        try {
            updateFileQueueAndStatuses();
            log.info("Initial file queue: {}", fileProcessingQueue);
        } catch (IOException e) {
            log.error("Error initializing file queue: ", e);
        }
    }

    private void updateFileQueueAndStatuses() throws IOException {
        List<String> filesToProcess = ftpService.listNewFiles();
        for (String fileName : filesToProcess) {
            if (!fileStatuses.containsKey(fileName) || !fileStatuses.get(fileName).isProcessed()) {
                fileProcessingQueue.add(fileName);
                fileStatuses.put(fileName, new FileProcessStatus());
            }
        }
    }
    @Override
    public void stop() {
        running = false;
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdownNow();
        }
        if (ftpService != null) {
            try {
                ftpService.disconnect();
            } catch (IOException e) {
                log.error("Error disconnecting FTP service: ", e);
            }
        }
    }
}