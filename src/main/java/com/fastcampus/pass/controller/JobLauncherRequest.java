package com.fastcampus.pass.controller;

import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

@Getter
@Setter
@ToString
public class JobLauncherRequest {

    private String name;
    private Properties jobParameters;

    public JobParameters getJobParameters() {
        return new JobParametersBuilder(this.jobParameters).toJobParameters();

    }

}
