package com.acme.customeringest.mongo;

import com.acme.customeringest.common.AppConstants;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = AppConstants.Mongo.CUSTOMERS_COLLECTION)
public class CustomerDocument {

  @Id private String customerId;
  private String phoneNumber;
  private AddressEmbed address;
  private Instant updatedAt;
  private String lastInboundTopic;
  private Integer lastInboundPartition;
  private Long lastInboundOffset;

  public String getCustomerId() {
    return customerId;
  }

  public void setCustomerId(String customerId) {
    this.customerId = customerId;
  }

  public String getPhoneNumber() {
    return phoneNumber;
  }

  public void setPhoneNumber(String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public AddressEmbed getAddress() {
    return address;
  }

  public void setAddress(AddressEmbed address) {
    this.address = address;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }

  public String getLastInboundTopic() {
    return lastInboundTopic;
  }

  public void setLastInboundTopic(String lastInboundTopic) {
    this.lastInboundTopic = lastInboundTopic;
  }

  public Integer getLastInboundPartition() {
    return lastInboundPartition;
  }

  public void setLastInboundPartition(Integer lastInboundPartition) {
    this.lastInboundPartition = lastInboundPartition;
  }

  public Long getLastInboundOffset() {
    return lastInboundOffset;
  }

  public void setLastInboundOffset(Long lastInboundOffset) {
    this.lastInboundOffset = lastInboundOffset;
  }
}
