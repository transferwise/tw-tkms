package com.transferwise.kafka.tkms.dao;

import static org.xerial.snappy.SnappyFramedOutputStream.DEFAULT_MIN_COMPRESSION_RATIO;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.streams.FastByteArrayOutputStream;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.Compression;
import com.transferwise.kafka.tkms.config.TkmsProperties.Compression.Algorithm;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers.Builder;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class TkmsTkmsMessageSerializer implements ITkmsMessageSerializer {

  protected static final int COMPRESSION_TYPE_NONE = 0;
  protected static final int COMPRESSION_TYPE_SNAPPY_FRAMED = 1;
  protected static final int COMPRESSION_TYPE_SNAPPY = 2;
  protected static final int COMPRESSION_TYPE_ZSTD = 3;

  @Autowired
  protected TkmsProperties properties;

  @Autowired
  protected ITkmsMetricsTemplate metricsTemplate;

  @Override
  public InputStream serialize(TkmsShardPartition shardPartition, TkmsMessage tkmsMessage) {
    return ExceptionUtils.doUnchecked(() -> {
      Message storedMessage = toStoredMessage(tkmsMessage);
      int serializedSize = storedMessage.getSerializedSize();

      FastByteArrayOutputStream os = new FastByteArrayOutputStream(serializedSize / 4);

      // 3 byte header for future use
      os.write(0);
      os.write(0);

      Compression compression = properties.getCompression();
      Algorithm algorithm = compression.getAlgorithm();
      if (algorithm == Algorithm.RANDOM) {
        algorithm = Algorithm.getRandom();
      }

      if (algorithm == Algorithm.NONE || serializedSize < compression.getMinSize()) {
        os.write(COMPRESSION_TYPE_NONE);
        storedMessage.writeTo(os);
      } else {
        if (algorithm == Algorithm.SNAPPY) {
          os.write(COMPRESSION_TYPE_SNAPPY);
          compressSnappy(compression, storedMessage, os);
        } else if (algorithm == Algorithm.SNAPPY_FRAMED) {
          os.write(COMPRESSION_TYPE_SNAPPY_FRAMED);
          compressSnappyFramed(compression, storedMessage, os);
        } else if (algorithm == Algorithm.ZSTD) {
          os.write(COMPRESSION_TYPE_ZSTD);
          compressZstd(compression, storedMessage, os);
        } else {
          throw new IllegalArgumentException("Compression algorithm " + algorithm + " is not supported.");
        }
        if (properties.isDebugEnabled()) {
          metricsTemplate.recordMessageCompression(shardPartition, algorithm, (double) os.size() / serializedSize);
        }
      }

      return os.toInputStream();
    });
  }

  @Override
  public Message deserialize(TkmsShardPartition shardPartition, InputStream is) {
    return ExceptionUtils.doUnchecked(() -> {
      byte h0 = (byte) is.read();
      byte h1 = (byte) is.read();
      byte h2 = (byte) is.read();

      InputStream decompressedStream = decompress(h2, is);
      try {
        long messageParsingStartNanoTime = System.nanoTime();
        StoredMessage.Message message = StoredMessage.Message.parseFrom(decompressedStream);
        if (properties.isDebugEnabled()) {
          metricsTemplate.recordStoredMessageParsing(shardPartition, messageParsingStartNanoTime);
        }
        return message;
      } finally {
        decompressedStream.close();
      }
    });
  }

  protected void compressSnappy(Compression compression, StoredMessage.Message storedMessage, OutputStream out) throws IOException {
    try (SnappyOutputStream compressOut = new SnappyOutputStream(out, compression.getBlockSize())) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressSnappyFramed(Compression compression, StoredMessage.Message storedMessage, OutputStream out) throws IOException {
    try (SnappyFramedOutputStream compressOut = new SnappyFramedOutputStream(out, compression.getBlockSize(), DEFAULT_MIN_COMPRESSION_RATIO)) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressZstd(Compression compression, StoredMessage.Message storedMessage, OutputStream out) throws IOException {
    try (ZstdOutputStream compressOut = new ZstdOutputStream(out, compression.getLevel())) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected InputStream decompress(byte header, InputStream dataStream) {
    int compressionType = getCompressionType(header);
    return ExceptionUtils.doUnchecked(() -> {
      if (compressionType == COMPRESSION_TYPE_NONE) {
        return dataStream;
      } else if (compressionType == COMPRESSION_TYPE_SNAPPY_FRAMED) {
        // Deprecated 0.4 version did not write checksums. 
        return new SnappyFramedInputStream(dataStream, false);
      } else if (compressionType == COMPRESSION_TYPE_ZSTD) {
        return new ZstdInputStream(dataStream);
      }
      return new SnappyInputStream(dataStream);
    });
  }

  /**
   * First 3 bits form a compression type/variant.
   */
  protected int getCompressionType(byte header) {
    return header & 7;
  }

  protected StoredMessage.Message toStoredMessage(TkmsMessage message) {
    StoredMessage.Headers headers = null;
    if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
      Builder builder = Headers.newBuilder();
      for (TkmsMessage.Header header : message.getHeaders()) {
        builder.addHeaders(StoredMessage.Header.newBuilder().setKey(header.getKey()).setValue(ByteString.copyFrom(header.getValue())).build());
      }
      headers = builder.build();
    }

    StoredMessage.Message.Builder storedMessageBuilder = StoredMessage.Message.newBuilder();
    storedMessageBuilder.setValue(ByteString.copyFrom(message.getValue()));
    if (headers != null) {
      storedMessageBuilder.setHeaders(headers);
    }
    if (message.getPartition() != null) {
      storedMessageBuilder.setPartition(UInt32Value.of(message.getPartition()));
    }
    if (message.getTimestamp() != null) {
      storedMessageBuilder.setTimestamp(UInt64Value.of(message.getTimestamp().toEpochMilli()));
    }
    if (message.getKey() != null) {
      storedMessageBuilder.setKey(message.getKey());
    }

    storedMessageBuilder.setInsertTimestamp(UInt64Value.of(System.currentTimeMillis()));

    return storedMessageBuilder.setTopic(message.getTopic()).build();
  }

}
