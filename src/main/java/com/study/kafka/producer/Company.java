package com.study.kafka.producer;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Company {
    private String name;
    private String address;
}
