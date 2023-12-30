package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class FTPService {
    private final String ftpServer;
    private final String userName;
    private final String password;
    private final FTPClient ftpClient;
    private Map<String, Long> fileTimestamps;
    private OffsetStorageReader offsetStorageReader;
    private long lastPollTime = 0;
    private boolean isInitialLoad = true;
    public FTPService(Map<String, String> props, OffsetStorageReader offsetStorageReader) {
        this.ftpServer = props.get("ftp.server");
        this.userName = props.get("user.name");
        this.password = props.get("password");
        this.ftpClient = new FTPClient();
        this.fileTimestamps = new HashMap<>();
        this.offsetStorageReader = offsetStorageReader;

    }

    public void connect() throws IOException {
        log.info("Connecting to an FTP server {}", this.ftpServer);
        ftpClient.connect(this.ftpServer);
        boolean login = ftpClient.login(this.userName, this.password);

        if (!login) {
            throw new IOException("Failed to login ftp server");
        }
        ftpClient.setControlEncoding("UTF-8");
    }

    public List<String> readFileByLine(String filePath) throws IOException {
        List<String> lines = new ArrayList<>();
        InputStream inputStream;

        try {
            inputStream = downloadFile(filePath);
            if (inputStream == null) {
                return lines;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            // 로그 기록
            e.printStackTrace();
        }

        return lines;
    }

    public boolean getIsInitialLoad() {
        return isInitialLoad;
    }
    public List<String> listAllFiles() throws IOException {
        List<String> files = new ArrayList<>();
        FTPFile[] ftpFiles = ftpClient.listFiles();
        for (FTPFile ftpFile : ftpFiles) {
            if (!ftpFile.isDirectory()) {
                files.add(ftpFile.getName());
            }
        }
        return files;
    }

    public List<String> listNewFiles() throws IOException {
        if (isInitialLoad) {
            isInitialLoad = false;
            return listAllFiles(); // 초기 실행 시 모든 파일을 반환
        }

        List<String> newFiles = new ArrayList<>();
        FTPFile[] files = ftpClient.listFiles();

        for (FTPFile file : files) {
            if (file.isDirectory()) {
                continue;
            }

            String fileName = file.getName();
            long modifiedTime = file.getTimestamp().getTimeInMillis();

            Long lastProcessedTime = fileTimestamps.getOrDefault(fileName, 0L);

            log.info("FileList Debug - 1 fileName: {} lastProcessedTime: {} / modifiedTime: {}", fileName, lastProcessedTime, modifiedTime);
            if (modifiedTime > lastPollTime && modifiedTime > lastProcessedTime) {
                newFiles.add(fileName);
                fileTimestamps.put(fileName, modifiedTime);
            }
        }

        lastPollTime = System.currentTimeMillis();

        return newFiles;
    }

    public void loadTimestampFromOffsets() {
        List<Map<String, String>> offsetRequest = fileTimestamps.keySet().stream()
                .map(fileName -> Collections.singletonMap("filename", fileName))
                .collect(Collectors.toList());

        Map<Map<String, String>, Map<String, Object>> offsets = offsetStorageReader.offsets(offsetRequest);
        for (Map.Entry<Map<String, String>, Map<String, Object>> entry : offsets.entrySet()) {
            String fileName = entry.getKey().get("filename");
            Map<String, Object> fileOffset = entry.getValue();
            if (fileOffset != null && fileOffset.containsKey("timestamp")) {
                Long timestamp = (Long) fileOffset.get("timestamp");
                fileTimestamps.put(fileName, timestamp);
            }
        }
    }

    public InputStream downloadFile(String fileName) throws IOException {
        return ftpClient.retrieveFileStream(fileName);
    }

    public void disconnect() throws IOException {
        log.info("Disconnect from the FTP server {}", this.ftpServer);
        if (ftpClient.isConnected()) {
            ftpClient.logout();
            ftpClient.disconnect();
        }
    }
}
