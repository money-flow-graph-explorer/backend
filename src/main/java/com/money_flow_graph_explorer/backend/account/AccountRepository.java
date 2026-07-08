package com.money_flow_graph_explorer.backend.account;

import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface AccountRepository extends Neo4jRepository<AccountNode, Long> {

    Optional<AccountNode> findByAccountId(Integer accountId);

    // Search by accountId (as string prefix) or customerId containing keyword
    @Query("""
            MATCH (a:Account)
            WHERE toString(a.accountId) CONTAINS $keyword
               OR toLower(a.customerId) CONTAINS toLower($keyword)
            RETURN a
            LIMIT 50
            """)
    List<AccountNode> searchByKeyword(@Param("keyword") String keyword);
}
