package com.transferwise.kafka.tkms.config;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.SocketSettings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("mongodb")
@Component
@Data
public class MongoDbProperties {
  private static final String SERVER_PORT_SEPARATOR = ":";

  @NotEmpty
  private List<String> replicaSet;
  @NotBlank
  private String rwUser;
  @SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
  private char[] rwUserPassword;
  @NotBlank
  private String dbName;
  @NotBlank
  private String authDbName;
  @NotNull
  private Integer connectionPoolMinSize;
  @NotNull
  private Integer connectionPoolMaxSize;
  @NotNull
  private Integer writeTimeoutSeconds;
  @NotNull
  private Integer readTimeoutSeconds;
  @NotNull
  private Integer connectTimeoutSeconds;

  public ConnectionPoolSettings getConnectionPoolSettings() {
    return ConnectionPoolSettings.builder()
        .minSize(connectionPoolMinSize)
        .maxSize(connectionPoolMaxSize)
        .build();
  }

  public SocketSettings getSocketSettings() {
    return SocketSettings.builder()
        .readTimeout(readTimeoutSeconds, TimeUnit.SECONDS)
        .connectTimeout(connectTimeoutSeconds, TimeUnit.SECONDS)
        .build();
  }

  public List<ServerAddress> getServerAddresses() {
    return replicaSet.stream().map(this::toServerAddress).collect(Collectors.toList());
  }

  private ServerAddress toServerAddress(String address) {
    String[] addressValues = address.split(SERVER_PORT_SEPARATOR);
    if (addressValues.length == 2) {
      String host = addressValues[0];
      int port = Integer.parseInt(addressValues[1]);
      return new ServerAddress(host, port);
    } else {
      return new ServerAddress(address);
    }
  }
}
