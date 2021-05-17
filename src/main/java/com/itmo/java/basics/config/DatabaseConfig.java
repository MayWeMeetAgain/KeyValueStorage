package com.itmo.java.basics.config;

public class DatabaseConfig {
    private final String workingPath;
    public static final String DEFAULT_WORKING_PATH = "db_files";

    public DatabaseConfig() {
        workingPath = DEFAULT_WORKING_PATH;
    }

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }

    public String getWorkingPath() {
        return workingPath;
    }
}
