package com.banking.transactionservice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;


public enum TransactionType {
    DEPOSIT,
    WITHDRAWAL,
    PAYMENT,
    TRANSFER
}