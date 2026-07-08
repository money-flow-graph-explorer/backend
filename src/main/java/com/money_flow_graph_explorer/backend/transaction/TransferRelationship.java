package com.money_flow_graph_explorer.backend.transaction;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Property;
import org.springframework.data.neo4j.core.schema.RelationshipProperties;
import org.springframework.data.neo4j.core.schema.TargetNode;
import com.money_flow_graph_explorer.backend.account.AccountNode;

@RelationshipProperties
@Getter
@Setter
public class TransferRelationship {

    @Id
    @GeneratedValue
    private Long internalId;

    @Property("txId")
    private Integer txId;

    @Property("amount")
    private Double amount;

    @Property("timestamp")
    private Integer timestamp;

    @Property("isFraud")
    private Boolean isFraud;

    @Property("alertId")
    private Integer alertId;

    @TargetNode
    private AccountNode target;
}
