package com.geekbrains.geekmarketwinter.repositories;

import com.geekbrains.geekmarketwinter.entites.FileMetaDTO;
import com.geekbrains.geekmarketwinter.repositories.interfaces.IFileMetaProvider;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.util.Collection;
import java.util.UUID;

@Repository
public class FileMetaProvider implements IFileMetaProvider {

    private static final String GET_FILES_META = "select hash, fileName as filename from filemeta" +
            " where subType =:subtype";

    private static final String GET_FILE_PATH_BY_HASH = "select fileName as filename from filemeta" +
            " where hash =:hash";

    private static final String SAVE_FILE_META_DATA = "insert into filemeta (hash, fileName, subType)" +
            " values (:hash, :fileName, :subtype)";

    private final Sql2o sql2o;

    public FileMetaProvider(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public String checkFileExists(UUID fileHash) {
       try(Connection connection = sql2o.open())  {
           return connection.createQuery(GET_FILE_PATH_BY_HASH, false)
                   .addParameter("hash", fileHash)
                   .executeScalar(String.class);
       }
    }

    @Override
    public void saveFileMeta(UUID hash, String fileName, int subType) {
        try(Connection connection = sql2o.open())  {
            connection.createQuery(SAVE_FILE_META_DATA, false)
                    .addParameter("hash", hash)
                    .addParameter("fileName", fileName)
                    .addParameter("subtype", subType)
                    .executeUpdate();
        }
    }

    @Override
    public Collection<FileMetaDTO> getMetaFiles(int subType) {
        try(Connection connection = sql2o.open())  {
            return connection.createQuery(GET_FILES_META, false)
                    .addParameter("subtype", subType)
                    .executeAndFetch(FileMetaDTO.class);
        }
    }
}
