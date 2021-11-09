package com.geekbrains.geekmarketwinter.repositories.interfaces;

import com.geekbrains.geekmarketwinter.entites.FileMetaDTO;

import java.util.Collection;
import java.util.UUID;

public interface IFileMetaProvider {
    String checkFileExists(UUID fileHash);

    void saveFileMeta(UUID Hash, String fileName, int subType);

    Collection<FileMetaDTO> getMetaFiles(int subType);
}
