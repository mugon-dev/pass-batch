package com.fastcampus.pass.job.pass;

import com.fastcampus.pass.repository.pass.PassEntity;
import com.fastcampus.pass.repository.pass.PassStatus;
import java.time.LocalDateTime;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExpirePassesJobConfig {

    public static final int CHUNK_SIZE = 5;
    // @EnableBatchProcessing로 인해 Bean으로 제공된 JobBuilderFactory, StepBuilderFactory
    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final EntityManagerFactory entityManagerFactory;

    public ExpirePassesJobConfig(JobBuilderFactory jobBuilderFactory,
        StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job expirePassesJob() {
        return this.jobBuilderFactory.get("expirePassesJob")
            .start(expirePassesStep())
            .build();
    }

    @Bean
    public Step expirePassesStep() {
        return this.stepBuilderFactory.get("expirePassesStep")
            .<PassEntity, PassEntity>chunk(CHUNK_SIZE)
            .reader(expirePassesItemReader())
            .processor(exprirePassesItemProcessor())
            .writer(expirePassesItemProcessor())
            .build();
    }


    /**
     * JpaCursorItemReader: JpaPagingItemReader만 지원하다가 spring 4.3에서 추가 페이징 기법보다 높은 성능으로 데이터 변경에 무관한
     * 무결성 조회가 가능
     */
    @Bean
    @StepScope
    public JpaCursorItemReader<PassEntity> expirePassesItemReader() {
        // 하나하나씩 업데이트하기 위해 Cursor 방식 사용
        return new JpaCursorItemReaderBuilder<PassEntity>()
            .name("expirePassesItemReader")
            .entityManagerFactory(entityManagerFactory)
            .queryString(
                "select p from PassEntity p where p.status = :status and p.endedAt <= :endedAt")
            .parameterValues(
                Map.of("status", PassStatus.PROGRESSED, "endedAt", LocalDateTime.now()))
            .build();
    }


    @Bean
    public ItemProcessor<PassEntity, PassEntity> exprirePassesItemProcessor() {
        return passEntity -> {
            passEntity.setStatus(PassStatus.EXPIRED);
            passEntity.setExpiredAt(LocalDateTime.now());
            return passEntity;
        };
    }

    /**
     * JpaItemWriter: JPA의 영속성 관리를 위해 EntityManager를 필수로 설정
     */
    @Bean
    public JpaItemWriter<PassEntity> expirePassesItemProcessor() {
        return new JpaItemWriterBuilder<PassEntity>()
            .entityManagerFactory(entityManagerFactory)
            .build();
    }

}
