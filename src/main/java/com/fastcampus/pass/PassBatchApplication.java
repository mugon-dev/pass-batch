package com.fastcampus.pass;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class PassBatchApplication {


    private final JobBuilderFactory jobBuilderFactory;


    private final StepBuilderFactory stepBuilderFactory;

    public PassBatchApplication(JobBuilderFactory jobBuilderFactory,
        StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public static void main(String[] args) {
        SpringApplication.run(PassBatchApplication.class, args);
    }

    @Bean
    public Step passStep() {
        return this.stepBuilderFactory.get("passStep").tasklet((contribution, chunkContext) -> {
            System.out.println("Execute PassStep");
            return null;
        }).build();
    }

    @Bean
    public Job passJob() {
        return this.jobBuilderFactory.get("passJob")
            .start(passStep())
            .build();
    }

}
