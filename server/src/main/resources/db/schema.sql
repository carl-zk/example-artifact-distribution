DROP TABLE IF EXISTS file_record;
CREATE TABLE file_record
(
    id          int AUTO_INCREMENT PRIMARY KEY,
    file_name   VARCHAR(255) NOT NULL,
    file_path   TEXT         NOT NULL,
    file_size   BIGINT       NOT NULL,
    sha256      VARCHAR(64),
    upload_time TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS TASK;
CREATE TABLE TASK
(
    "ID"           int AUTO_INCREMENT PRIMARY KEY,
    "AGENT_ID"     VARCHAR(100)  NOT NULL,
    "AGENT_IP"     VARCHAR(45),
    "FILE_ID"      INTEGER       NOT NULL,
    "FILE_NAME"    VARCHAR(255)  NOT NULL,
    "FILE_SIZE"    BIGINT        NOT NULL,
    "SAVE_TO_DIR"  VARCHAR(1024) NOT NULL,
    "STATUS"       VARCHAR(20)   NOT NULL,
    "CREATED_TIME" TIMESTAMP     NOT NULL default current_timestamp
);