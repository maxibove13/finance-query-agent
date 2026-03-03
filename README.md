# finance-query-agent

AI-powered natural language query agent for financial databases. Uses Pydantic AI with predefined parameterized query tools and a constrained SQL fallback.

## Installation

```bash
pip install finance-query-agent
```

## Quick Start

```python
from finance_query_agent import create_agent, SchemaMapping, TableMapping, AmountConvention

schema = SchemaMapping(
    transactions=TableMapping(
        table="transactions",
        columns={
            "date": "created_at",
            "amount": "amount",
            "description": "description",
            "user_id": "user_id",
            "currency": "currency",
            "account_id": "account_id",
        },
        amount_convention=AmountConvention(sign_means_expense="negative"),
    ),
    categories=TableMapping(
        table="categories",
        columns={"id": "id", "name": "name"},
        user_scoped=False,
    ),
    accounts=TableMapping(
        table="accounts",
        columns={"id": "id", "name": "name", "user_id": "user_id"},
    ),
)

agent = create_agent(
    db_url="postgresql://user:pass@localhost/mydb",
    schema=schema,
    model="openai:gpt-4o",
)

result = await agent.run("How much did I spend on groceries last month?", user_id="user-123")
print(result.answer)
```

## License

MIT
