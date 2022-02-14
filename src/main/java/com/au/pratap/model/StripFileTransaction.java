package com.au.pratap.model;

import lombok.*;

import java.io.Serializable;


@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class StripFileTransaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter
    @Setter
    private String source;

    @Getter
    @Setter
    private String messageText;

    @Getter
    @Setter
    private String bsb;

    @Getter
    @Setter
    private String accountNumber;

    @Getter
    @Setter
    private String accountId;
}
