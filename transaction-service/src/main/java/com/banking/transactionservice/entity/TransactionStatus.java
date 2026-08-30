package com.banking.transactionservice.entity;


/*
* transaction life cycle
* PENDING->PROCESSING -> COMPLETED(clean transaction)
*                     ->PENDING_VERIFICATION (suspicious transaction)
*                                ->COMPLETED (verified)
*                      ->FAILED (rejected)
*                       ->FLAGGED
* */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
