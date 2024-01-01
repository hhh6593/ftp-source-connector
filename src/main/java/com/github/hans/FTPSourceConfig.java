package com.github.hans;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FTPSourceConfig extends AbstractConfig {

    // 구성 옵션 정의
    public static final String FTP_SERVER_CONFIG = "ftp.server";
    public static final String USER_NAME_CONFIG = "user.name";
    public static final String PASSWORD_CONFIG = "password";

    private static final String KAFKA_TOPIC = "topic";
    private static final String FTP_SERVER_DOC = "FTP server.";
    private static final String USER_NAME_DOC = "FTP username.";
    private static final String PASSWORD_DOC = "FTP password.";
    private static final String KAFKA_TOPIC_DOC = "Record Destination";

    public FTPSourceConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(FTP_SERVER_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FTP_SERVER_DOC)
                .define(USER_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, USER_NAME_DOC)
                .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, PASSWORD_DOC)
                .define(KAFKA_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC);
    }

    public String getFtpServer() {
        return this.getString(FTP_SERVER_CONFIG);
    }

    public String getUserName() {
        return this.getString(USER_NAME_CONFIG);
    }

    public String getPassword() {
        return this.getPassword(PASSWORD_CONFIG).value();
    }

    public String getKafkaTopic() {
        return this.getString(KAFKA_TOPIC);
    }
}
