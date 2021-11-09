package com.geekbrains.geekmarketwinter.services;

import com.geekbrains.geekmarketwinter.entites.FileMetaDTO;
import com.geekbrains.geekmarketwinter.repositories.interfaces.IFileMetaProvider;
import com.geekbrains.geekmarketwinter.repositories.interfaces.IFileSystemProvider;
import com.geekbrains.geekmarketwinter.services.interfaces.IFileStoreService;
import com.geekbrains.geekmarketwinter.utils.HashHelper;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.UUID;

@Component
public class FileStoreService implements IFileStoreService {

    @Autowired
    IFileSystemProvider systemProvider;

    @Autowired
    IFileMetaProvider fileMetaProvider;

    @Override
    public String storeFile(byte[] content, String originalFilename, int subFileType) throws IOException, NoSuchAlgorithmException {
        final UUID md5 = HashHelper.getMd5Hash(content);

        // Сохранение нового файла
        String filename = fileMetaProvider.checkFileExists(md5);
        if (filename == null) {
            filename = systemProvider.storeFile(content, md5, originalFilename);
        }

        //  Добавление записи в БД если:
        //  - UUID отсутствует в БД
        //  - При наличии данного UUID отличается originalFilename или subFileType
        Collection<FileMetaDTO> fileMetaDTO = fileMetaProvider.getMetaFile (md5, originalFilename, subFileType);
        if (fileMetaDTO.isEmpty ()){
            fileMetaProvider.saveFileMeta(md5, originalFilename, subFileType);
        }

        return originalFilename;
    }

    @Override
    public byte[] getFile(UUID md5) throws IOException {
       String filename = fileMetaProvider.checkFileExists(md5);
       String ext = FilenameUtils.getExtension(filename);
       String fullFileName = md5.toString() + "." + ext;
       return systemProvider.getFile(fullFileName);
    }

    @Override
    public Collection<FileMetaDTO> getMetaFiles(int subtype) {
        return fileMetaProvider.getMetaFiles(subtype);
    }
}
