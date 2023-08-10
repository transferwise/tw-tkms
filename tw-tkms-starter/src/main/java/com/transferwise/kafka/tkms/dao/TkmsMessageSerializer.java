package com.transferwise.kafka.tkms.dao;

import static org.xerial.snappy.SnappyFramedOutputStream.DEFAULT_MIN_COMPRESSION_RATIO;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.CompressionAlgorithm;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.Compression;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers.Builder;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class TkmsMessageSerializer implements ITkmsMessageSerializer {

  protected static final int COMPRESSION_TYPE_NONE = 0;
  protected static final int COMPRESSION_TYPE_SNAPPY_FRAMED = 1;
  protected static final int COMPRESSION_TYPE_SNAPPY = 2;
  protected static final int COMPRESSION_TYPE_ZSTD = 3;
  protected static final int COMPRESSION_TYPE_LZ4 = 4;
  protected static final int COMPRESSION_TYPE_GZIP = 5;

  @Autowired
  protected TkmsProperties properties;

  @Autowired
  protected ITkmsMetricsTemplate metricsTemplate;

  @Override
  public InputStream serialize(TkmsShardPartition shardPartition, TkmsMessage tkmsMessage) throws IOException {
    Message storedMessage = toStoredMessage(tkmsMessage);
    int serializedSize = storedMessage.getSerializedSize();

    var os = new UnsynchronizedByteArrayOutputStream(serializedSize / 4);

    // 3 byte header for future use
    os.write(0);
    os.write(0);

    Compression compression = properties.getCompression(shardPartition.getShard());
    CompressionAlgorithm compressionAlgorithm = compression.getAlgorithm();
    int minCompressableSize = compression.getMinSize();
    Integer compressionBlockSize = compression.getBlockSize();
    Integer compressionLevel = compression.getLevel();

    TkmsMessage.Compression tkmsMessageCompression = tkmsMessage.getCompression();
    if (tkmsMessageCompression != null) {
      minCompressableSize = 0;
      compressionAlgorithm = tkmsMessageCompression.getAlgorithm();
      compressionBlockSize = tkmsMessageCompression.getBlockSize();
      compressionLevel = tkmsMessageCompression.getLevel();
    }

    if (serializedSize < minCompressableSize) {
      compressionAlgorithm = CompressionAlgorithm.NONE;
    } else if (compressionAlgorithm == CompressionAlgorithm.RANDOM) {
      compressionAlgorithm = CompressionAlgorithm.getRandom();
    }

    if (compressionAlgorithm == CompressionAlgorithm.NONE) {
      os.write(COMPRESSION_TYPE_NONE);
      storedMessage.writeTo(os);
    } else if (compressionAlgorithm == CompressionAlgorithm.SNAPPY) {
      os.write(COMPRESSION_TYPE_SNAPPY);
      compressSnappy(storedMessage, os, compressionBlockSize);
    } else if (compressionAlgorithm == CompressionAlgorithm.SNAPPY_FRAMED) {
      os.write(COMPRESSION_TYPE_SNAPPY_FRAMED);
      compressSnappyFramed(storedMessage, os, compressionBlockSize);
    } else if (compressionAlgorithm == CompressionAlgorithm.ZSTD) {
      os.write(COMPRESSION_TYPE_ZSTD);
      compressZstd(storedMessage, os, compressionLevel);
    } else if (compressionAlgorithm == CompressionAlgorithm.LZ4) {
      os.write(COMPRESSION_TYPE_LZ4);
      compressLz4(storedMessage, os, compressionBlockSize);
    } else if (compressionAlgorithm == CompressionAlgorithm.GZIP) {
      os.write(COMPRESSION_TYPE_GZIP);
      compressGzip(storedMessage, os);
    } else {
      throw new IllegalArgumentException("Compression compressionAlgorithm " + compressionAlgorithm + " is not supported.");
    }

    metricsTemplate.recordMessageSerialization(shardPartition, compressionAlgorithm, serializedSize, os.size());

    os.flush();

    var serializedBytes = os.toByteArray();

    if (properties.isValidateSerialization(shardPartition.getShard())) {
      var deSerializedMessage = deserialize(shardPartition, new UnsynchronizedByteArrayInputStream(serializedBytes));

      if (!Arrays.equals(deSerializedMessage.getValue().toByteArray(), tkmsMessage.getValue())
          || !areSimilar(deSerializedMessage.getKey(), tkmsMessage.getKey())
          || !areSimilar(deSerializedMessage.getTopic(), tkmsMessage.getTopic())
      ) {
        throw new IllegalStateException("Data corruption detected. Serialized and deserialized messages are not equal.");
      }
    }

    return new UnsynchronizedByteArrayInputStream(serializedBytes);
  }

  private boolean areSimilar(String s0, String s1) {
    if ((s0 == null || s0.length() == 0) && (s1 == null || s1.length() == 0)) {
      return true;
    }
    return Objects.equals(s0, s1);
  }

  @Override
  public Message deserialize(TkmsShardPartition shardPartition, InputStream is) throws IOException {
    // Reserved header #0
    is.read();
    // Reserved header #1
    is.read();
    byte h2 = (byte) is.read();

    var decompressedStream = decompress(h2, is);
    try {
      return StoredMessage.Message.parseFrom(decompressedStream);
    } finally {
      decompressedStream.close();
    }
  }

  protected void compressSnappy(StoredMessage.Message storedMessage, OutputStream out, Integer blockSize) throws IOException {
    try (var compressOut = new SnappyOutputStream(out, blockSize == null ? 32 * 1024 : blockSize)) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressSnappyFramed(StoredMessage.Message storedMessage, OutputStream out, Integer blockSize) throws IOException {
    try (var compressOut = new SnappyFramedOutputStream(out,
        blockSize == null ? SnappyFramedOutputStream.DEFAULT_BLOCK_SIZE : blockSize, DEFAULT_MIN_COMPRESSION_RATIO)) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressZstd(StoredMessage.Message storedMessage, OutputStream out, Integer level) throws IOException {
    try (var compressOut = level == null ? new ZstdOutputStream(out) : new ZstdOutputStream(out, level)) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressLz4(StoredMessage.Message storedMessage, OutputStream out, Integer blockSize) throws IOException {
    try (var compressOut = new LZ4BlockOutputStream(out, blockSize == null ? 1 << 16 : blockSize,
        LZ4Factory.fastestJavaInstance().fastCompressor())) {
      storedMessage.writeTo(compressOut);
    }
  }

  protected void compressGzip(StoredMessage.Message storedMessage, OutputStream out) throws IOException {
    try (var compressOut = new GZIPOutputStream(out)) {
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
      } else if (compressionType == COMPRESSION_TYPE_LZ4) {
        return new LZ4BlockInputStream(dataStream, LZ4Factory.fastestJavaInstance().fastDecompressor());
      } else if (compressionType == COMPRESSION_TYPE_GZIP) {
        return new GZIPInputStream(dataStream);
      }

      return new SnappyInputStream(dataStream);
    });
  }

  /**
   * First 3 bits form a compression type/variant.
   */
  protected int getCompressionType(byte header) {
    return header & 0b111;
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
