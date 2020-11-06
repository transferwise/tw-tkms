package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Message;
import java.io.InputStream;

public interface ITkmsMessageSerializer {

  InputStream serialize(TkmsShardPartition shardPartition, TkmsMessage tkmsMessage);

  Message deserialize(TkmsShardPartition shardPartition, InputStream is);
}
