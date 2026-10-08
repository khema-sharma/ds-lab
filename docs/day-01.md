# Answers

## Questions

1. What number would tell you the system is not keeping up, and where would you read it?

   A performance test report triggered by CI/CD process shows 99th percentile response time greater than 200 ms.

2. What single failure are you promising to recover from on this one node, and how long may that take?

   A write failure will get recovered within 5 seconds.

3. If a new engineer had to add a field to a stored record next month, what would make that change expensive, and why is a wiki page a weaker guard than the test you just wrote?

   The logic that writes the record to disk is complex, with lots of branches. Wiki page is a weaker guard because it gets decayed over a period of time and even if kept up-to-get it can get too huge making it difficult for new engineers to follow up. On the other hand writing a readable test case will ensure that the critical paths of the system will be covered daily.
