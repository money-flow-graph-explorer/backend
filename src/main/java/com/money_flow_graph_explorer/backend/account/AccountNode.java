package com.money_flow_graph_explorer.backend.account;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;

@Node("Account")
@Getter
@Setter
public class AccountNode {

    @Id
    @GeneratedValue
    private Long internalId;

    @Property("accountId")
    private Integer accountId;

    @Property("customerId")
    private String customerId;

    @Property("initBalance")
    private Double initBalance;

    @Property("country")
    private String country;

    @Property("accountType")
    private String accountType;

    @Property("isFraud")
    private Boolean isFraud;

    @Property("txBehaviorId")
    private Integer txBehaviorId;
}
