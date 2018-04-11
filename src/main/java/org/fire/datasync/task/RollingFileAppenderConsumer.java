package org.fire.datasync.task;

import com.alibaba.fastjson.JSON;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * User: fire
 * Date: 2018-04-08
 */
public class RollingFileAppenderConsumer extends DispatcherConsumer {
    private static final Logger log = LoggerFactory.getLogger(RollingFileAppenderConsumer.class);

    // 记录变更时间戳
    private static final String DATA_TS_KEY = "__bigdata_ts";
    // 记录变更类型：插入、删除、更新
    private static final String DATA_OP_KEY = "__bigdata_status";
    // 单个文件最大容量，单位byte
    private static final int MAX_FILESIZE = 100 * 1024 * 1024;

    private String filename;
    private FileOutputStream fos;
    private int writtenBytes;
    private int maxFileSize = MAX_FILESIZE;

    public RollingFileAppenderConsumer(String filename) {
        initialize(filename);
    }

    @Override
    protected void onMessage(OPLogMessage message) {
        DBObject data = message.getData();
        data.put(DATA_TS_KEY, getSequence());
        data.put(DATA_OP_KEY, message.getType());
        append(JSON.toJSONString(data));
    }

    private long getSequence() {
        return System.currentTimeMillis();
    }

    public void initialize(String filename) {
        this.filename = filename;
        mkdirIfNotExist();
        openFile();
    }

    private void mkdirIfNotExist() {
        File file = new File("data");
        if (!file.exists()) {
            file.mkdir();
        }
    }

    public void append(String s) {
        if (triggerRolling()) {
            rollover();
        }

        try {
            writeOut(s);
            log.debug(">>>PRODUCE:{}", s);
        } catch (IOException e) {
            log.error("文件写入失败{}", s, e);
        }
    }

    private void writeOut(String s) throws IOException {
        byte[] buffer = convertToBytes(s);
        fos.write(buffer);
        writtenBytes += buffer.length;
    }

    private byte[] convertToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private boolean triggerRolling() {
        return writtenBytes >= maxFileSize;
    }

    public void rollover() {
        beforeRollover(filename);
        reopenFile();
    }

    protected void beforeRollover(String filename) {
    }

    private void reopenFile() {
        if (fos != null) {
            try {
                fos.close();
                writtenBytes = 0;
            } catch (IOException e) {
                log.error("无法关闭文件", e);
            }
        }

        File file = new File(filename);
        if (file.exists()) {
            file.delete();
        }

        openFile();
    }

    private void openFile() {
        try {
            fos = new FileOutputStream(filename, true);
            log.debug("打开新文件：{}", filename);
        } catch (Exception e) {
            log.error("无法打开文件", e);
        }
    }
}
