package com.notificationservice.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private final OutboxRepository repository;

    @Value("${outbox.poll.batch-size:100}")
    private int batchSize;

    @Value("${outbox.claim.lease-ms:300000}")
    private long leaseMs;

    @Transactional
    public List<OutboxClaim> claimBatch() {
        LocalDateTime now = LocalDateTime.now();
        List<OutboxEvent> events = repository.findClaimable(now, PageRequest.of(0, batchSize));
        LocalDateTime leaseUntil = now.plusNanos(leaseMs * 1_000_000);

        return events.stream().map(event -> {
            String token = UUID.randomUUID().toString();
            event.setClaimToken(token);
            event.setClaimUntil(leaseUntil);
            return new OutboxClaim(event.getId(), event.getEventId(), event.getTopic(),
                    event.getMessageKey(), event.getPayload(), token);
        }).toList();
    }

    @Transactional
    public boolean markPublished(OutboxClaim claim) {
        return repository.markAsPublished(claim.id(), claim.claimToken(), LocalDateTime.now()) == 1;
    }

    @Transactional
    public boolean renew(OutboxClaim claim) {
        return repository.renewClaim(claim.id(), claim.claimToken(),
                LocalDateTime.now().plusNanos(leaseMs * 1_000_000)) == 1;
    }

    @Transactional
    public void releaseAfterFailure(OutboxClaim claim) {
        repository.releaseAfterFailure(claim.id(), claim.claimToken());
    }
}
