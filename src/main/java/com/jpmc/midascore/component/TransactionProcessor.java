package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);
    
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    
    public TransactionProcessor(UserRepository userRepository, TransactionRepository transactionRepository, DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }
    
    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void processTransaction(Transaction transaction) {
        logger.info("Processing transaction: {}", transaction);
        
        // Validate sender exists
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Transaction discarded - Invalid sender ID: {}", transaction.getSenderId());
            return;
        }
        
        // Validate recipient exists
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Transaction discarded - Invalid recipient ID: {}", transaction.getRecipientId());
            return;
        }
        
        // Validate sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Transaction discarded - Insufficient balance. Sender {} has balance {} but transaction amount is {}", 
                sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }
        
        // Process the transaction
        float incentiveAmount = incentiveService.fetchIncentive(transaction).getAmount();
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);


        // Save updated balances
        userRepository.save(sender);
        userRepository.save(recipient);
        
        // Record the transaction
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);
        
        logger.info("Transaction processed successfully. {} -> {}: ${}", 
            sender.getName(), recipient.getName(), transaction.getAmount());
    }
}
