package com.fastcampus.pass.repository.pass;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BulkPassRepository extends JpaRepository<BulkPassEntity, Integer> {

    // WHERE status = :status AND startedAt > :startedAT
    List<BulkPassEntity> findByStatusAndStartedAtGreaterThan(BulkPassStatus status,
        LocalDateTime startedAt);

}