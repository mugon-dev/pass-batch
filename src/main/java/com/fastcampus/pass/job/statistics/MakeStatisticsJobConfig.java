package com.fastcampus.pass.job.statistics;

import com.fastcampus.pass.repository.booking.BookingEntity;
import com.fastcampus.pass.repository.statistics.StatisticsEntity;
import com.fastcampus.pass.repository.statistics.StatisticsRepository;
import com.fastcampus.pass.util.LocalDateTimeUtils;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Slf4j
@Configuration
public class MakeStatisticsJobConfig {

    private final int CHUNK_SIZE = 10;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final StatisticsRepository statisticsRepository;
    private final MakeDailyStatisticsTasklet makeDailyStatisticsTasklet;
    private final MakeWeeklyStatisticsTasklet makeWeeklyStatisticsTasklet;

    public MakeStatisticsJobConfig(JobBuilderFactory jobBuilderFactory,
        StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory,
        StatisticsRepository statisticsRepository,
        MakeDailyStatisticsTasklet makeDailyStatisticsTasklet,
        MakeWeeklyStatisticsTasklet makeWeeklyStatisticsTasklet) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
        this.statisticsRepository = statisticsRepository;
        this.makeDailyStatisticsTasklet = makeDailyStatisticsTasklet;
        this.makeWeeklyStatisticsTasklet = makeWeeklyStatisticsTasklet;
    }

    @Bean
    public Job makeStatisticsJob() {
        // statistics 데이터 넣는 부분
        Flow addStatisticsFlow = new FlowBuilder<Flow>("addStatisticsFlow")
            .start(addStatisticsStep())
            .build();

        // 일간 statistics csv 생성
        Flow makeDailyStatisticsFlow = new FlowBuilder<Flow>("makeDailyStatisticsFlow")
            .start(makeDailyStatisticsStep())
            .build();

        // 주간 statistics csv 생성
        Flow makeWeeklyStatisticsFlow = new FlowBuilder<Flow>("makeWeeklyStatisticsFlow")
            .start(makeWeeklyStatisticsStep())
            .build();

        // 병렬로 처리하는 플로우
        Flow parallelMakeStatisticsFlow = new FlowBuilder<Flow>("parallelMakeStatisticsFlow")
            .split(new SimpleAsyncTaskExecutor())
            // 같이 진행할 플로우 선언
            .add(makeDailyStatisticsFlow, makeWeeklyStatisticsFlow)
            .build();

        return this.jobBuilderFactory.get("makeStatisticsJob")
            .start(addStatisticsFlow)
            .next(parallelMakeStatisticsFlow)
            .build()
            .build();
    }

    @Bean
    public Step addStatisticsStep() {
        return this.stepBuilderFactory.get("addStatisticsStep")
            .<BookingEntity, BookingEntity>chunk(CHUNK_SIZE)
            .reader(addStatisticsItemReader(null, null))
            .writer(addStatisticsItemWriter())
            .build();
    }

    @Bean
    @StepScope // jobParameters를 사용하기 위해 선언
    public JpaCursorItemReader<BookingEntity> addStatisticsItemReader(
        @Value("#{jobParameters[from]}") String fromString,
        @Value("#{jobParameters[to]}") String toString) {
        final LocalDateTime from = LocalDateTimeUtils.parse(fromString);
        final LocalDateTime to = LocalDateTimeUtils.parse(toString);

        return new JpaCursorItemReaderBuilder<BookingEntity>()
            .name("usePassesItemReader")
            .entityManagerFactory(entityManagerFactory)
            // JobParameter를 받아 종료 일시(endedAt) 기준으로 통계 대상 예약(Booking)을 조회합니다.
            .queryString("select b from BookingEntity b where b.endedAt between :from and :to")
            .parameterValues(Map.of("from", from, "to", to))
            .build();
    }

    @Bean
    public ItemWriter<BookingEntity> addStatisticsItemWriter() {
        return bookingEntities -> {
            // 시간으로 묶기위해 key => LocalDateTiem
            // LinkedHashMap으로 시간순 정렬
            Map<LocalDateTime, StatisticsEntity> statisticsEntityMap = new LinkedHashMap<>();

            for (BookingEntity bookingEntity : bookingEntities) {
                final LocalDateTime statisticsAt = bookingEntity.getStatisticsAt();
                StatisticsEntity statisticsEntity = statisticsEntityMap.get(statisticsAt);

                if (statisticsEntity == null) {
                    statisticsEntityMap.put(statisticsAt, StatisticsEntity.create(bookingEntity));

                } else {
                    statisticsEntity.add(bookingEntity);

                }

            }
            final List<StatisticsEntity> statisticsEntities = new ArrayList<>(
                statisticsEntityMap.values());
            statisticsRepository.saveAll(statisticsEntities);
            log.info("### addStatisticsStep 종료");

        };
    }

    @Bean
    public Step makeDailyStatisticsStep() {
        // 병렬로 처리
        return this.stepBuilderFactory.get("makeDailyStatisticsStep")
            .tasklet(makeDailyStatisticsTasklet)
            .build();
    }

    @Bean
    public Step makeWeeklyStatisticsStep() {
        // 병렬로 처리
        return this.stepBuilderFactory.get("makeWeeklyStatisticsStep")
            .tasklet(makeWeeklyStatisticsTasklet)
            .build();
    }
}
