package com.transferwise.kafka.tkms;

public class AlgorithmErrorException extends RuntimeException {

  static final long serialVersionUID = 1L;

  public AlgorithmErrorException(String message) {
    super("Algorithm error detected: " + message);
  }
}
