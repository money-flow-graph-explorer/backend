// =============================================================
// Money Flow Graph Explorer — Initial Data Load
// =============================================================
// Run via cypher-shell inside the running container:
//
//   docker exec -it money-flow-neo4j cypher-shell \
//     -u neo4j -p password123 \
//     --file /var/lib/neo4j/import/../import/load.cypher
//
// OR copy this file into the container first:
//
//   docker cp src/main/resources/cypher/load.cypher \
//     money-flow-neo4j:/var/lib/neo4j/import/load.cypher
//
//   docker exec -it money-flow-neo4j cypher-shell \
//     -u neo4j -p password123 \
//     --file /var/lib/neo4j/import/load.cypher
//
// The data/ directory is mounted at /var/lib/neo4j/import inside
// the container, so LOAD CSV paths use "file:///accounts.csv" etc.
// =============================================================

// ----- 0. Constraints & Indexes -----

CREATE CONSTRAINT account_id_unique IF NOT EXISTS
  FOR (a:Account)
  REQUIRE a.accountId IS UNIQUE;

CREATE INDEX transfer_alert_id IF NOT EXISTS
  FOR ()-[r:TRANSFER]-()
  ON (r.alertId);

CREATE INDEX transfer_timestamp IF NOT EXISTS
  FOR ()-[r:TRANSFER]-()
  ON (r.timestamp);

CREATE CONSTRAINT alert_id_unique IF NOT EXISTS
  FOR (a:Alert)
  REQUIRE a.alertId IS UNIQUE;

// ----- 1. Load Account nodes -----
// accounts.csv columns:
// ACCOUNT_ID,CUSTOMER_ID,INIT_BALANCE,COUNTRY,ACCOUNT_TYPE,IS_FRAUD,TX_BEHAVIOR_ID

LOAD CSV WITH HEADERS FROM 'file:///accounts.csv' AS row
CALL {
  WITH row
  MERGE (a:Account {accountId: toInteger(row.ACCOUNT_ID)})
  SET
    a.customerId   = row.CUSTOMER_ID,
    a.initBalance  = toFloat(row.INIT_BALANCE),
    a.country      = row.COUNTRY,
    a.accountType  = row.ACCOUNT_TYPE,
    a.isFraud      = (row.IS_FRAUD = 'true' OR row.IS_FRAUD = 'True'),
    a.txBehaviorId = toInteger(row.TX_BEHAVIOR_ID)
} IN TRANSACTIONS OF 1000 ROWS;

// ----- 2. Load TRANSFER relationships -----
// transactions.csv columns:
// TX_ID,SENDER_ACCOUNT_ID,RECEIVER_ACCOUNT_ID,TX_TYPE,TX_AMOUNT,TIMESTAMP,IS_FRAUD,ALERT_ID

LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
CALL {
  WITH row
  MATCH (sender:Account {accountId: toInteger(row.SENDER_ACCOUNT_ID)})
  MATCH (receiver:Account {accountId: toInteger(row.RECEIVER_ACCOUNT_ID)})
  CREATE (sender)-[t:TRANSFER {
    txId:      toInteger(row.TX_ID),
    amount:    toFloat(row.TX_AMOUNT),
    timestamp: toInteger(row.TIMESTAMP),
    isFraud:   (row.IS_FRAUD = 'true' OR row.IS_FRAUD = 'True'),
    alertId:   toInteger(row.ALERT_ID)
  }]->(receiver)
} IN TRANSACTIONS OF 5000 ROWS;

// ----- 3. Load Alert nodes -----
// alerts.csv columns:
// ALERT_ID,ALERT_TYPE,IS_FRAUD,TX_ID,SENDER_ACCOUNT_ID,RECEIVER_ACCOUNT_ID,TX_TYPE,TX_AMOUNT,TIMESTAMP

LOAD CSV WITH HEADERS FROM 'file:///alerts.csv' AS row
CALL {
  WITH row
  MERGE (al:Alert {alertId: toInteger(row.ALERT_ID)})
  SET al.alertType = row.ALERT_TYPE
} IN TRANSACTIONS OF 1000 ROWS;

// ----- Done -----
// Verify:
//   MATCH (a:Account) RETURN count(a);         -- expect 10000
//   MATCH ()-[t:TRANSFER]->() RETURN count(t); -- expect 1323234
//   MATCH (al:Alert) RETURN count(al);          -- expect 391
