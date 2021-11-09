package com.geekbrains.geekmarketwinter.entites;

import java.util.UUID;

public class FileMetaDTO {
    private UUID hash;

    private String fileName;

    public FileMetaDTO(String fileName) {
        this.fileName = fileName;
    }

    public UUID getHash() {
        return hash;
    }

    public void setHash(UUID hash) {
        this.hash = hash;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
