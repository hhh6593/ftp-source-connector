package com.github.hans;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

@Slf4j
public class FTPSourceConnector extends SourceConnector {

    private FTPSourceConfig configProps;
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting FTP Source Connector {}", props);
        configProps = new FTPSourceConfig(props);
        String ftpServer = configProps.getString(FTPSourceConfig.FTP_SERVER_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FTPSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();

        // 각 태스크에 대한 구성 생성
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(configProps.originalsStrings());
            // 필요하다면 여기서 각 태스크에 대한 추가 구성을 설정할 수 있습니다.
            configs.add(taskProps);
        }

        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping FTP Source Connector");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("ftp.server", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "FTP server address")
                .define("user.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "FTP username")
                .define("password", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "FTP password")
                .define("topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target Topic");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
