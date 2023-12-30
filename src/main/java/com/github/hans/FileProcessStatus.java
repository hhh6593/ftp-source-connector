package com.github.hans;
public class FileProcessStatus {
    private long lastProcessedTime;
    private boolean isProcessed;

    public FileProcessStatus(long lastProcessedTime, boolean isProcessed) {
        this.lastProcessedTime = lastProcessedTime;
        this.isProcessed = isProcessed;
    }
    public FileProcessStatus() {
        this.lastProcessedTime = 0;
        this.isProcessed = false;
    }

    // Getters and Setters
    public long getLastProcessedTime() {
        return lastProcessedTime;
    }

    public void setLastProcessedTime(long lastProcessedTime) {
        this.lastProcessedTime = lastProcessedTime;
    }

    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(boolean processed) {
        isProcessed = processed;
    }
}
