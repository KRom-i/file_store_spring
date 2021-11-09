package com.geekbrains.geekmarketwinter.repositories.interfaces;

import java.io.IOException;
import java.util.UUID;

public interface IFileSystemProvider {
    // получение файла
    byte[] getFile(String hash) throws IOException;

    // сохранение файла
    String storeFile(byte[] content, UUID md5, String fileName) throws IOException;

    // удаление файла
    void deleteFile(String fileHash) throws IOException;

}
