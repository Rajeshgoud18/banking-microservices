package com.banking.accountservice.repository;

import com.banking.accountservice.entity.Account;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;


public interface AccountRepository extends JpaRepository<Account, Long> {
    boolean existsByEmail(@NotBlank(message = "Email is required") String email);

    boolean existsByAccountNumber(String accountNumber);

    Optional<Account> findByAccountNumber(String accountNumber);
}
