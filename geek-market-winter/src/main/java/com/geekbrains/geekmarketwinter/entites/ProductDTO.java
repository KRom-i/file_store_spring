package com.geekbrains.geekmarketwinter.entites;

import java.util.UUID;

public class ProductDTO {
    private UUID hash;
    private Double fileSize;

    public UUID getHash() {
        return hash;
    }

    public void setHash(UUID hash) {
        this.hash = hash;
    }

    public Double getFileSize() {
        return fileSize;
    }

    public void setFileSize(Double fileSize) {
        this.fileSize = fileSize;
    }
}
