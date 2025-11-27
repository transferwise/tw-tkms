package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import java.util.List;

public interface ITkmsMessagePoller {

  List<MessageRecord> pullMessages(TkmsShardPartition shardPartition, long earliestMessageId, int batchSize);

}
